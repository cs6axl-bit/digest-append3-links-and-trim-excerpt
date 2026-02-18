# frozen_string_literal: true

# name: digest-append3-links-and-trim-excerpt
# version: 1.8
# about: Appends isdigest=1, u=<user_id>, dayofweek=<base64url(email)>, email_id=<20-digit> to internal links in Activity Summary (digest) emails.
#        PLUS (v1.4): Rewrites ALL EXTERNAL links inside post excerpt bodies to /content?u=<base64url(final_url)> so email clients show only local links.
#        Optimized for high volume: single Nokogiri parse, cheap pre-checks, and separate switches for HTML/TEXT trimming.
#        HTML trim preserves markup by trimming text nodes in-place.
#        NEW (v1.2): Count topics ONLY before "Popular Posts" section.
#        NEW (v1.3): Count topics by UNIQUE topic_id (from /t/.../<id>) to avoid overcount when one topic renders multiple excerpt blocks.
#        NEW (v1.3): HTML trimming now trims FORWARD (keeps early paragraphs), not reverse-deleting later nodes.
#        NEW (v1.3): normalize_spaces collapses ALL whitespace so we don't cut early at line breaks.
#        NEW (v1.5): FIX topic counting: no longer relies solely on /t/... links (tracked links break it).
#                  - "Popular Posts" boundary detection tightened (headings/strong only; must be a likely header)
#        NEW (v1.6): FIX trimming when only ONE topic exists:
#                  - If topic ids are observed (data-topic-id OR /t/.../<id>), treat count>=1 as definitive.
#                  - Fallback excerpt-node counting only used when NO topic ids are found.
#                  - If excerpt nodes exist but no ids, assume 1 topic.
#        NEW (v1.7): Added option to remove ALL DOM after cut point (removes trailing images/elements).
#        NEW (v1.8): Make trailing-image removal optional via switch.

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

    # rewrite links inside post bodies (excerpts) to /content?u=<base64url(url)>
    ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS = true
    CONTENT_REDIRECTOR_PATH = "/content"
    CONTENT_REDIRECTOR_PARAM = "u"

    ENABLE_TRIM_HTML_PART = true
    HTML_MAX_CHARS        = 300

    ENABLE_TRIM_TEXT_PART = true
    TEXT_MAX_CHARS        = 300

    # NEW (v1.8):
    # If true: when we cut inside an excerpt, delete ALL nodes that come after the cut point
    #          (this removes trailing <img>/<figure>/<picture>/etc).
    # If false: only text nodes are removed (images/elements after the cut may remain).
    ENABLE_REMOVE_TRAILING_ELEMENTS_AFTER_CUT = true

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

    # v1.5: tighten boundary detection so we don’t accidentally match random body text
    POPULAR_MARKER_TAGS = "h1,h2,h3,h4,h5,h6,strong,b"

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
      h
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

    # v1.5: scoped doc is ONLY before a likely section header for Popular Posts.
    def self.scoped_doc_before_popular(doc)
      return doc unless doc

      marker =
        doc.css(POPULAR_MARKER_TAGS).find do |n|
          next false unless node_text_matches_popular?(n)

          # Avoid matching long paragraphs that happen to mention the words.
          # We expect section headers to be short-ish.
          txt = normalize_spaces(n.text)
          txt_len = txt.length
          txt_len > 0 && txt_len <= 60
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
      doc # fail-open
    end

    # ============================================================
    # Topic counting (BEFORE "Popular Posts") - robust (v1.6+)
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

      # (1) Best: data-topic-id exists in many digest templates
      ids1 =
        scope
          .css("[data-topic-id]")
          .map { |n| n["data-topic-id"].to_s.strip }
          .reject(&:empty?)
          .uniq

      return ids1.size if ids1.size >= 1

      # (2) Next: /t/.../<id> links (works when not tracked/redirected)
      ids2 =
        scope
          .css("a[href]")
          .map { |a| extract_topic_id_from_href(a["href"], base) }
          .compact
          .uniq

      return ids2.size if ids2.size >= 1

      # (3) Fallback ONLY when no ids found:
      excerpt_nodes =
        HTML_EXCERPT_SELECTORS
          .flat_map { |sel| scope.css(sel).to_a }
          .uniq

      # Multiple excerpt blocks can still be ONE topic -> assume 1 if any exist
      return 1 if excerpt_nodes.size >= 1

      0
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

      # if we observe ANY topic ids, that is definitive (1 => 1)
      return ids.size if ids.size >= 1

      # fallback: if we can’t find ids but there are multiple “topic URL” blocks, treat as multiple
      topic_url_blocks = (cutoff ? blocks[0...cutoff] : blocks).count { |b| b.to_s =~ TEXT_TOPIC_URL_REGEX }
      return topic_url_blocks if topic_url_blocks > 1

      0
    rescue
      999 # fail-open
    end

    # ============================================================
    # Optional: remove EVERYTHING after cut point (elements too)
    # ============================================================

    def self.remove_everything_after_point!(container_node, point_node)
      return if container_node.nil? || point_node.nil?

      cur = point_node

      # Walk upward toward container_node.
      # At each level, remove all siblings AFTER the current node.
      while cur && cur != container_node
        parent = cur.parent
        break if parent.nil?

        sib = cur.next_sibling
        while sib
          nxt = sib.next_sibling
          sib.remove
          sib = nxt
        end

        cur = parent
      end
    rescue
      nil
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

          if ENABLE_REMOVE_TRAILING_ELEMENTS_AFTER_CUT
            # Remove ALL DOM after cut point (images/elements too)
            remove_everything_after_point!(node, tn)
          else
            # Legacy behavior: only remove remaining TEXT nodes (images/elements may remain)
            text_nodes[(idx + 1)..-1].to_a.each(&:remove)
          end

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
        # fallback (no /content rewrite here)
        if ENABLE_LINK_REWRITE
          html_part.body = rewrite_links_regex(body, user, email_id, base)
        end
        return
      end

      dayofweek_val = encoded_email(user)
      doc = Nokogiri::HTML(body)
      changed = false

      # 1) rewrite INTERNAL links: add isdigest/u/dayofweek/email_id
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

      # 2) rewrite ALL links INSIDE excerpt bodies to /content?u=<base64url(final_url)>
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

              abs0 = absolute_url_from_href(href, base)
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

              redirect_abs = make_content_redirector_url(abs0, base)
              next if redirect_abs.nil?

              a["href"] = redirect_abs
              changed = true
            end
          end
        end
      end

      # If only one UNIQUE topic exists BEFORE "Popular Posts", skip excerpt trimming
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

      # If only one UNIQUE topic exists BEFORE "Popular Posts", skip text excerpt trimming entirely
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
