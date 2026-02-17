# name: digest-append3-links-and-trim-excerpt
# version: 1.4
# about: Appends isdigest=1, u=<user_id>, dayofweek=<base64url(email)>, email_id=<20-digit> to internal links in Activity Summary (digest) emails.
#        PLUS (v1.4): Rewrites ALL EXTERNAL links inside post excerpt bodies to /content?u=<base64url(final_url)> so email clients show only local links.
#        Optimized for high volume: single Nokogiri parse, cheap pre-checks, and separate switches for HTML/TEXT trimming.
#        HTML trim preserves markup by trimming text nodes in-place.
#        NEW (v1.2): Count topics ONLY before "Popular Posts" section.
#        NEW (v1.3): Count topics by UNIQUE topic_id (from /t/.../<id>) to avoid overcount when one topic renders multiple excerpt blocks.
#        NEW (v1.3): HTML trimming now trims FORWARD (keeps early paragraphs), not reverse-deleting later nodes.
#        NEW (v1.3): normalize_spaces collapses ALL whitespace so we don't cut early at line breaks.

after_initialize do
  require_dependency "user_notifications"
  require "uri"
  require "base64"
  require "securerandom"

  begin
    require "nokogiri"
  rescue LoadError
    Nokogiri = nil
  end

  module ::DigestAppendData
    # ============================================================
    # CONFIG
    # ============================================================

    ENABLE_LINK_REWRITE = true

    # NEW (v1.4): rewrite links inside post bodies (excerpts) to /content?u=<base64url(url)>
    ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS = true
    CONTENT_REDIRECTOR_PATH = "/content"
    CONTENT_REDIRECTOR_PARAM = "u"

    ENABLE_TRIM_HTML_PART = true
    HTML_MAX_CHARS        = 300

    ENABLE_TRIM_TEXT_PART = true
    TEXT_MAX_CHARS        = 300

    HTML_EXCERPT_SELECTORS = [
      ".digest-post-excerpt",
      ".post-excerpt",
      ".excerpt",
      ".topic-excerpt",
      "div[itemprop='articleBody']"
    ]

    NEVER_TOUCH_HREF_SUBSTRINGS = [
      "/email/unsubscribe",
      "/my/preferences"
    ]

    TEXT_TOPIC_URL_REGEX = %r{(^|\s)(https?://\S+)?/t/[^ \n]+}i
    TEXT_NEVER_TRIM_KEYWORDS = [
      "unsubscribe",
      "/email/unsubscribe",
      "preferences",
      "/my/preferences"
    ]

    # Heading text that marks the start of the "Popular Posts" section.
    POPULAR_POSTS_MARKERS = [
      "popular posts",
      "popular topics"
    ]

    # ============================================================
    # Hook
    # ============================================================

    def digest(user, opts = {})
      super.tap do |message|
        email_id = ::DigestAppendData.generate_email_id
        ::DigestAppendData.process_html_part!(message, user, email_id)
        ::DigestAppendData.trim_digest_text_part!(message)
      end
    end

    # ============================================================
    # Helpers
    # ============================================================

    def self.generate_email_id
      SecureRandom.random_number(10**20).to_s.rjust(20, "0")
    end

    def self.encoded_email(user)
      email = user&.email.to_s
      return "" if email.empty?
      Base64.urlsafe_encode64(email, padding: false)
    end

    # IMPORTANT: collapse ALL whitespace (including newlines) into spaces
    def self.normalize_spaces(s)
      s.to_s.gsub(/\s+/, " ").strip
    end

    def self.contains_any?(haystack, needles)
      h = haystack.to_s
      needles.any? { |n| h.include?(n) }
    end

    def self.smart_trim_plain(text, max_chars)
      t = normalize_spaces(text)
      return t if t.length <= max_chars

      limit = [max_chars - 1, 0].max
      cut = t[0, limit]
      if (idx = cut.rindex(/\s/))
        cut = cut[0, idx]
      end
      cut.rstrip + "…"
    end

    def self.base64url_encode(s)
      Base64.urlsafe_encode64(s.to_s, padding: false)
    rescue
      ""
    end

    def self.make_content_redirector_url(final_url, base)
      token = base64url_encode(final_url)
      return nil if token.to_s.empty?
      "#{base}#{CONTENT_REDIRECTOR_PATH}?#{CONTENT_REDIRECTOR_PARAM}=#{token}"
    end

    def self.absolute_url_from_href(href, base)
      h = href.to_s.strip
      return nil if h.empty?
      return (base + h) if h.start_with?("/")
      return h
    end

    def self.http_url?(url)
      u = URI.parse(url)
      u.is_a?(URI::HTTP) || u.is_a?(URI::HTTPS)
    rescue
      false
    end

    # ============================================================
    # "Popular Posts" boundary helpers
    # ============================================================

    def self.node_text_matches_popular?(node)
      t = normalize_spaces(node&.text).downcase
      return false if t.empty?
      POPULAR_POSTS_MARKERS.any? { |m| t.include?(m) }
    rescue
      false
    end

    def self.scoped_doc_before_popular(doc)
      return doc unless doc

      marker =
        doc.css("h1,h2,h3,h4,h5,h6,strong,b,td,th,p,div,span").find do |n|
          node_text_matches_popular?(n)
        end

      return doc unless marker

      body = doc.at("body")
      return doc unless body

      # Find the top-level body child that contains the marker (or is the marker)
      top = marker
      while top && top.parent && top.parent != body
        top = top.parent
      end

      parts = []
      body.children.each do |child|
        break if child == top
        parts << child.to_html
      end

      Nokogiri::HTML(parts.join("\n"))
    rescue
      doc # fail-open: behave as before
    end

    # ============================================================
    # Topic counting (BEFORE "Popular Posts") by UNIQUE topic_id
    # ============================================================

    def self.extract_topic_id_from_href(href, base)
      h = href.to_s
      return nil if h.empty?
      return nil unless h.include?("/t/")

      path =
        if h.start_with?("/")
          h
        elsif h.start_with?(base)
          h.sub(base, "")
        else
          h
        end

      m = path.match(%r{/t/(?:[^/]+/)?(\d+)}i)
      m ? m[1] : nil
    rescue
      nil
    end

    def self.count_topics_in_html_doc(doc)
      scope = scoped_doc_before_popular(doc)
      base = Discourse.base_url

      ids =
        scope
          .css("a[href]")
          .map { |a| extract_topic_id_from_href(a["href"], base) }
          .compact
          .uniq

      ids.size
    rescue
      999 # fail-open: do NOT accidentally skip trimming
    end

    def self.count_topics_in_text_blocks(blocks)
      cutoff = blocks.find_index do |b|
        t = normalize_spaces(b).downcase
        POPULAR_POSTS_MARKERS.any? { |m| t.include?(m) }
      end

      scoped_text = cutoff ? blocks[0...cutoff].join("\n\n") : blocks.join("\n\n")
      ids = scoped_text.scan(%r{/t/(?:[^/\s]+/)?(\d+)}i).flatten.uniq
      ids.size
    rescue
      999 # fail-open
    end

    # ============================================================
    # HTML: trim in-place FORWARD (keeps early paragraphs), then cuts once
    # ============================================================

    def self.trim_html_node_in_place!(node, max_chars)
      full_norm = normalize_spaces(node.text.to_s)
      return false if full_norm.length <= max_chars

      text_nodes = node.xpath(".//text()[not(ancestor::script) and not(ancestor::style)]").to_a
      return false if text_nodes.empty?

      budget = max_chars
      trimming_started = false

      text_nodes.each_with_index do |tn, idx|
        raw = tn.text.to_s
        norm = normalize_spaces(raw)
        next if norm.empty?

        if !trimming_started
          if norm.length <= budget
            budget -= norm.length
            next
          end

          # Cut inside THIS node (keeps earlier content + adds ellipsis)
          tn.content = smart_trim_plain(raw, budget)
          trimming_started = true

          # Remove all remaining text nodes AFTER this one
          text_nodes[(idx + 1)..-1].to_a.each(&:remove)
          break
        else
          tn.remove
        end
      end

      true
    rescue
      false
    end

    # ============================================================
    # HTML processing (single Nokogiri pass)
    # ============================================================

    def self.process_html_part!(message, user, email_id)
      return if message.nil?
      return unless ENABLE_LINK_REWRITE || ENABLE_TRIM_HTML_PART

      html_part =
        if message.respond_to?(:html_part) && message.html_part
          message.html_part
        else
          message
        end

      body = html_part.body&.decoded
      return if body.nil? || body.empty?

      base = Discourse.base_url

      if ENABLE_LINK_REWRITE && !body.include?('href="') && !body.include?("href='")
        return unless ENABLE_TRIM_HTML_PART
      end

      if ENABLE_TRIM_HTML_PART
        selector_hints = ["digest-post-excerpt", "post-excerpt", "topic-excerpt", "itemprop=\"articleBody\"", "itemprop='articleBody'", "excerpt"]
        has_trim_hint = selector_hints.any? { |h| body.include?(h) }
        return if !has_trim_hint && !ENABLE_LINK_REWRITE
      end

      if !Nokogiri
        # keep your old fallback (no /content rewrite here)
        if ENABLE_LINK_REWRITE
          html_part.body = rewrite_links_regex(body, user, email_id, base)
        end
        return
      end

      dayofweek_val = encoded_email(user)
      doc = Nokogiri::HTML(body)
      changed = false

      # 1) rewrite INTERNAL links (as before): add isdigest/u/dayofweek/email_id
      if ENABLE_LINK_REWRITE
        doc.css("a[href]").each do |a|
          href = a["href"].to_s.strip
          next if href.empty?
          next if href.start_with?("mailto:", "tel:", "sms:", "#")

          is_relative = href.start_with?("/")
          is_internal = href.start_with?(base)
          next unless is_relative || is_internal

          next if contains_any?(href, NEVER_TOUCH_HREF_SUBSTRINGS)

          begin
            uri = URI.parse(is_relative ? (base + href) : href)
          rescue URI::InvalidURIError
            next
          end

          next unless uri.scheme.nil? || uri.scheme == "http" || uri.scheme == "https"

          params = URI.decode_www_form(uri.query || "")

          added = false
          unless params.any? { |k, _| k == "isdigest" }
            params << ["isdigest", "1"]
            added = true
          end
          unless params.any? { |k, _| k == "u" }
            params << ["u", user.id.to_s]
            added = true
          end
          if !dayofweek_val.empty? && !params.any? { |k, _| k == "dayofweek" }
            params << ["dayofweek", dayofweek_val]
            added = true
          end
          if email_id && !email_id.empty? && !params.any? { |k, _| k == "email_id" }
            params << ["email_id", email_id]
            added = true
          end

          next unless added

          uri.query = URI.encode_www_form(params)
          a["href"] = uri.to_s
          changed = true
        end
      end

      # 2) NEW: rewrite ALL links INSIDE excerpt bodies to /content?u=<base64url(final_url)>
      if ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS
        excerpt_nodes =
          HTML_EXCERPT_SELECTORS
            .flat_map { |sel| doc.css(sel).to_a }
            .uniq

        if excerpt_nodes.any?
          excerpt_nodes.each do |node|
            node.css("a[href]").each do |a|
              href = a["href"].to_s.strip
              next if href.empty?
              next if href.start_with?("mailto:", "tel:", "sms:", "#")
              next if contains_any?(href, NEVER_TOUCH_HREF_SUBSTRINGS)

              # don't double-wrap already wrapped /content
              begin
                abs0 = absolute_url_from_href(href, base)
              rescue
                next
              end
              next if abs0.nil?
              next unless http_url?(abs0)

              # skip if it's already /content
              begin
                u0 = URI.parse(abs0)
                if u0.host == URI.parse(base).host && u0.path == CONTENT_REDIRECTOR_PATH
                  next
                end
              rescue
                # ignore
              end

              # IMPORTANT: use the FINAL URL currently in href (which may already include isdigest/u/dayofweek/email_id for internal)
              redirect_abs = make_content_redirector_url(abs0, base)
              next if redirect_abs.nil?

              a["href"] = redirect_abs
              changed = true
            end
          end
        end
      end

      # If only one UNIQUE topic_id exists BEFORE "Popular Posts", skip excerpt trimming
      if ENABLE_TRIM_HTML_PART
        topic_count = count_topics_in_html_doc(doc)
        do_trim = topic_count > 1

        if do_trim
          nodes =
            HTML_EXCERPT_SELECTORS
              .flat_map { |sel| doc.css(sel).to_a }
              .uniq

          nodes.each do |node|
            begin
              hrefs = node.css("a[href]").map { |x| x["href"].to_s }
              next if hrefs.any? { |h| contains_any?(h, NEVER_TOUCH_HREF_SUBSTRINGS) }
            rescue
              next
            end

            if trim_html_node_in_place!(node, HTML_MAX_CHARS)
              changed = true
            end
          end
        end
      end

      html_part.body = doc.to_html if changed
    rescue => e
      Rails.logger.warn("digest-append3-links-and-trim-excerpt HTML process failed: #{e.class}: #{e.message}")
      nil
    end

    def self.rewrite_links_regex(body, user, email_id, base)
      dayofweek_val = encoded_email(user)

      body.gsub(/href="(#{Regexp.escape(base)}[^"]*|\/[^"]*)"/) do
        url = Regexp.last_match(1)

        next %{href="#{url}"} if url.include?("isdigest=") ||
                                url.include?("email_id=") ||
                                contains_any?(url, NEVER_TOUCH_HREF_SUBSTRINGS)

        joiner = url.include?("?") ? "&" : "?"
        extra  = "isdigest=1&u=#{user.id}"
        extra += "&dayofweek=#{dayofweek_val}" unless dayofweek_val.empty?
        extra += "&email_id=#{email_id}" if email_id && !email_id.empty?

        %{href="#{url}#{joiner}#{extra}"}
      end
    end

    # ============================================================
    # TEXT trimming (topic-body-only)
    # ============================================================

    def self.trim_digest_text_part!(message)
      return if message.nil?
      return unless ENABLE_TRIM_TEXT_PART
      return unless message.respond_to?(:text_part) && message.text_part

      tp = message.text_part
      text = tp.body&.decoded
      return if text.nil? || text.empty?

      t = text.to_s.gsub(/\r\n?/, "\n")
      blocks = t.split(/\n{2,}/)

      # If only one UNIQUE topic_id exists BEFORE "Popular Posts", skip text excerpt trimming entirely
      topic_count = count_topics_in_text_blocks(blocks)
      return if topic_count <= 1

      changed = false

      blocks.each_with_index do |blk, i|
        b = blk.to_s
        next if b.strip.empty?

        b_down = b.downcase
        next if TEXT_NEVER_TRIM_KEYWORDS.any? { |kw| b_down.include?(kw.downcase) }

        prev = (i > 0) ? blocks[i - 1].to_s : ""
        prev_has_topic_url = !!(prev =~ TEXT_TOPIC_URL_REGEX)
        next unless prev_has_topic_url

        norm_len = normalize_spaces(b).length
        next if norm_len <= TEXT_MAX_CHARS

        blocks[i] = smart_trim_plain(b, TEXT_MAX_CHARS)
        changed = true
      end

      tp.body = blocks.join("\n\n") if changed
    rescue => e
      Rails.logger.warn("digest-append3-links-and-trim-excerpt TEXT trim failed: #{e.class}: #{e.message}")
      nil
    end
  end

  class ::UserNotifications
    prepend ::DigestAppendData
  end
end
