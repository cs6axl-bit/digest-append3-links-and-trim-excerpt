# frozen_string_literal: true

# name: digest-append3-links-and-trim-excerpt
# version: 1.7.6
# about: Appends isdigest=1, u=<user_id>, dayofweek=<base64url(email)>, email_id=<20-digit> to internal links in Activity Summary (digest) emails.
#        PLUS (v1.4): Rewrites ALL links inside post excerpt bodies to /content?u=<base64url(final_url)> so email clients show only local links.
#        PLUS (v1.5): If there is only one p.digest-topic-name in HTML, skip excerpt trimming entirely (fast + accurate).
#        FIX  (v1.6): Topic counting computed BEFORE /content rewrites; Popular Posts boundary uses document order (robust even if whole email is one big table).
#        PLUS (v1.7): Trim to MIN(max_chars, first visual line break).
#        FIX  (v1.7.1): Line-break trimming now RESPECTS ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES (keep images/objects when false).
#        PLUS (v1.7.6): For /content rewrite (post-body links), append aff_sub2 and subid2 where value is "#{user_id}-#{topic_id_of_current_digest_topic}" (topic id derived from surrounding digest HTML, not from destination URL).

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

    # Rewrite links inside post bodies (excerpts) to /content?u=<base64url(url)>
    ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS = true
    CONTENT_REDIRECTOR_PATH  = "/content"
    CONTENT_REDIRECTOR_PARAM = "u"

    # Append tracking params to FINAL destination URL BEFORE encoding (post-body links only)
    ENABLE_APPEND_TRACKING_PARAMS_TO_POST_BODY_LINKS = true

    # These params will be appended to the FINAL destination URL (before base64url-encoding into /content?u=...)
    # Value will be "#{user_id}-#{topic_id_context}" when topic_id_context is known, else "#{user_id}".
    TRACKING_PARAMS_TO_APPEND = ["aff_sub2", "subid2"]

    ENABLE_TRIM_HTML_PART = true
    HTML_MAX_CHARS        = 300

    ENABLE_TRIM_TEXT_PART = true
    TEXT_MAX_CHARS        = 300

    # If true, when we trim an excerpt we ALSO remove any trailing HTML nodes
    # (images, tables, oneboxes, etc.) after the cut point.
    # If false, we remove ONLY trailing text nodes (images/objects may remain).
    ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES = true

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

    # Trim to min(max_chars, first newline if it occurs before max_chars).
    def self.smart_trim_plain(text, max_chars)
      raw = text.to_s.gsub(/\r\n?/, "\n")

      nl = raw.index("\n")
      linebreak_forced = nl && nl > 0 && nl < max_chars

      if linebreak_forced
        kept = raw[0, nl]
        kept_norm = normalize_spaces(kept)
        return kept_norm if kept_norm.empty?
        return kept_norm.end_with?("…") ? kept_norm : (kept_norm + "…")
      end

      full_norm = normalize_spaces(raw)
      return full_norm if full_norm.length <= max_chars

      limit = [max_chars - 1, 0].max
      cut = full_norm[0, limit]

      if (idx = cut.rindex(/\s/))
        cut = cut[0, idx]
      end

      cut = cut.rstrip
      return cut if cut.empty?
      cut.end_with?("…") ? cut : (cut + "…")
    rescue
      normalize_spaces(text)
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
    # Topic ID extraction
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

    # Try to infer "current digest topic id" for a given excerpt node.
    #
    # Strategy (cheap, robust):
    # 1) If any ancestor has data-topic-id / data-topicid / topic-id, use it.
    # 2) Look for the closest preceding p.digest-topic-name that has a /t/... link.
    # 3) As fallback, look for a preceding /t/... link anywhere (closest) and use that id.
    def self.topic_id_context_for_excerpt(node, base)
      return nil unless node

      # 1) ancestor attributes
      begin
        anc = node.at_xpath("ancestor-or-self::*[@data-topic-id or @data-topicid or @topic-id][1]")
        if anc
          v = anc["data-topic-id"] || anc["data-topicid"] || anc["topic-id"]
          vv = v.to_s.strip
          return vv if vv.match?(/^\d+$/)
        end
      rescue
        # ignore
      end

      # 2) closest preceding digest-topic-name
      begin
        a = node.at_xpath("preceding::p[contains(concat(' ', normalize-space(@class), ' '), ' digest-topic-name ')][1]//a[@href][1]")
        if a && a["href"]
          id = extract_topic_id_from_href(a["href"], base)
          return id if id
        end
      rescue
        # ignore
      end

      # 3) closest preceding /t/ anchor
      begin
        a2 = node.at_xpath("preceding::a[contains(@href, '/t/')][1]")
        if a2 && a2["href"]
          id2 = extract_topic_id_from_href(a2["href"], base)
          return id2 if id2
        end
      rescue
        # ignore
      end

      nil
    end

    def self.user_topic_value(user_id, topic_id_context)
      uid = user_id.to_s
      return "" if uid.empty?
      tid = topic_id_context.to_s
      return uid if tid.empty?
      "#{uid}-#{tid}"
    end

    # Append aff_sub2/subid to destination URL BEFORE encoding into /content
    def self.append_tracking_params(url, user_id, topic_id_context)
      return url if url.to_s.empty?
      val = user_topic_value(user_id, topic_id_context)
      return url if val.empty?

      uri = URI.parse(url)
      return url unless uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)

      params = URI.decode_www_form(uri.query || "")

      TRACKING_PARAMS_TO_APPEND.each do |k|
        next if k.to_s.empty?
        next if params.any? { |kk, _| kk == k }
        params << [k, val]
      end

      uri.query = URI.encode_www_form(params)
      uri.to_s
    rescue
      url
    end

    # ============================================================
    # "Popular Posts" boundary helpers (robust)
    # ============================================================

    def self.node_text_matches_popular?(node)
      t = normalize_spaces(node&.text).downcase
      return false if t.empty?
      POPULAR_POSTS_MARKERS.any? { |m| t.include?(m) }
    rescue
      false
    end

    def self.find_popular_marker_node(doc)
      return nil unless doc
      doc.css("h1,h2,h3,h4,h5,h6,strong,b,td,th,p,div,span").find do |n|
        node_text_matches_popular?(n)
      end
    rescue
      nil
    end

    def self.before_marker?(node, marker)
      return true unless marker
      node < marker
    rescue
      true
    end

    # ============================================================
    # Primary topic counting BEFORE Popular Posts (v1.6 fix)
    # ============================================================

    def self.primary_topic_count_before_popular(doc)
      return nil unless doc
      base   = Discourse.base_url
      marker = find_popular_marker_node(doc)

      keys = []

      # 1) Prefer digest topic-name blocks
      doc.css("p.digest-topic-name").each do |p|
        next unless before_marker?(p, marker)

        a = p.at_css("a[href]")
        id = a ? extract_topic_id_from_href(a["href"], base) : nil

        title = normalize_spaces(p.text)
        if id
          keys << "id:#{id}"
        elsif !title.empty?
          keys << "t:#{title}"
        end
      end

      keys = keys.compact.uniq
      return keys.size if keys.any?

      # 2) Fallback: scan anchors for /t/ ids (before marker)
      ids = []
      doc.css("a[href]").each do |a|
        next unless before_marker?(a, marker)
        id = extract_topic_id_from_href(a["href"], base)
        ids << id if id
      end

      ids = ids.compact.uniq
      return ids.size if ids.any?

      nil
    rescue
      nil
    end

    # ============================================================
    # HTML trimming helpers
    # ============================================================

    def self.append_ellipsis_to_last_text!(node)
      tn = node.xpath(".//text()[not(ancestor::script) and not(ancestor::style)]")
               .to_a
               .reverse
               .find { |t| !normalize_spaces(t.text).empty? }
      return false unless tn

      s = tn.text.to_s.rstrip
      return false if s.end_with?("…")

      tn.content = s + "…"
      true
    rescue
      false
    end

    def self.text_before_node(root, stop_node)
      out = +""
      root.traverse do |n|
        break if n == stop_node
        if n.text?
          out << n.text
          out << " "
        end
      end
      out
    rescue
      ""
    end

    def self.has_content_after_boundary?(boundary, root)
      cur = boundary
      while cur && cur != root
        sib = cur.next_sibling
        while sib
          if sib.element?
            return true
          elsif sib.text?
            return true unless normalize_spaces(sib.text).empty?
          end
          sib = sib.next_sibling
        end
        cur = cur.parent
      end
      false
    rescue
      true
    end

    def self.remove_following_siblings_up_to_root!(boundary, root)
      cur = boundary
      while cur && cur != root
        cur.xpath("following-sibling::node()").each(&:remove)
        cur = cur.parent
      end
      true
    rescue
      false
    end

    def self.end_node_for_kept_region(boundary)
      return boundary unless boundary
      last_desc = boundary.xpath(".//node()").to_a.last
      last_desc || boundary
    rescue
      boundary
    end

    # Remove ONLY text nodes after a given end node (keeps images/objects).
    def self.remove_text_nodes_after_end!(root, end_node)
      return false unless root && end_node
      root.xpath(".//text()[not(ancestor::script) and not(ancestor::style)]").each do |tn|
        next unless tn > end_node
        tn.remove
      end
      true
    rescue
      false
    end

    # ============================================================
    # Line-break trimming (respects ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES)
    # ============================================================

    def self.trim_html_at_first_line_break!(node, max_chars)
      return false unless node

      # 1) <br> is the strongest "line break" signal
      br = node.at_css("br")
      if br
        before_len = normalize_spaces(text_before_node(node, br)).length
        if before_len > 0 && before_len < max_chars && has_content_after_boundary?(br, node)
          if ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES
            remove_following_siblings_up_to_root!(br, node)
          else
            remove_text_nodes_after_end!(node, br) # keep images/objects
          end
          br.remove
          return true
        end
      end

      # 2) End of first paragraph/list item boundary
      boundary = node.at_css("p,li")
      if boundary
        kept_len = normalize_spaces(boundary.text).length
        if kept_len > 0 && kept_len < max_chars && has_content_after_boundary?(boundary, node)
          if ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES
            remove_following_siblings_up_to_root!(boundary, node)
          else
            end_node = end_node_for_kept_region(boundary)
            remove_text_nodes_after_end!(node, end_node) # keep images/objects
          end
          return true
        end
      end

      false
    rescue
      false
    end

    # ============================================================
    # HTML: trim in-place
    # ============================================================

    def self.trim_html_node_in_place!(node, max_chars)
      break_trimmed = trim_html_at_first_line_break!(node, max_chars)

      full_norm = normalize_spaces(node.text.to_s)
      return break_trimmed if full_norm.length <= max_chars

      text_nodes = node.xpath(".//text()[not(ancestor::script) and not(ancestor::style)]").to_a
      return break_trimmed if text_nodes.empty?

      budget = max_chars
      trimming_started = false

      text_nodes.each_with_index do |tn, idx|
        raw  = tn.text.to_s
        norm = normalize_spaces(raw)
        next if norm.empty?

        if !trimming_started
          if norm.length <= budget
            budget -= norm.length
            next
          end

          tn.content = smart_trim_plain(raw, budget)
          trimming_started = true

          if ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES
            remove_following_siblings_up_to_root!(tn, node)
          else
            text_nodes[(idx + 1)..-1].to_a.each(&:remove)
          end

          break
        else
          tn.remove
        end
      end

      if break_trimmed && !trimming_started
        append_ellipsis_to_last_text!(node)
      end

      trimming_started || break_trimmed
    rescue
      false
    end

    # ============================================================
    # HTML processing (single Nokogiri pass)
    # ============================================================

    def self.process_html_part!(message, user, email_id)
      return if message.nil?
      return unless ENABLE_LINK_REWRITE || ENABLE_TRIM_HTML_PART || ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS

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
        selector_hints = [
          "digest-post-excerpt",
          "post-excerpt",
          "topic-excerpt",
          "itemprop=\"articleBody\"",
          "itemprop='articleBody'",
          "excerpt",
          "digest-topic-name"
        ]
        has_trim_hint = selector_hints.any? { |h| body.include?(h) }
        return if !has_trim_hint && !ENABLE_LINK_REWRITE && !ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS
      end

      if !Nokogiri
        if ENABLE_LINK_REWRITE
          html_part.body = rewrite_links_regex(body, user, email_id, base)
        end
        return
      end

      dayofweek_val = encoded_email(user)
      doc = Nokogiri::HTML(body)
      changed = false

      # v1.6: compute primary topic count NOW (before any /content rewrites)
      primary_topic_count = primary_topic_count_before_popular(doc)

      # Skip trim ONLY when we are confident it's exactly 1.
      skip_trim = (primary_topic_count == 1)

      # If unknown (nil), fail-open to trimming when enabled.
      do_trim = primary_topic_count.nil? ? true : (primary_topic_count > 1)

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

        excerpt_nodes.each do |node|
          # derive topic-id context ONCE per excerpt node
          topic_ctx = topic_id_context_for_excerpt(node, base)

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
              b0 = URI.parse(base)
              if u0.host == b0.host && u0.path == CONTENT_REDIRECTOR_PATH
                next
              end
            rescue
              # ignore
            end

            final_dest =
              if ENABLE_APPEND_TRACKING_PARAMS_TO_POST_BODY_LINKS
                append_tracking_params(abs0, user.id, topic_ctx)
              else
                abs0
              end

            redirect_abs = make_content_redirector_url(final_dest, base)
            next if redirect_abs.nil?

            a["href"] = redirect_abs
            changed = true
          end
        end
      end

      # 3) HTML excerpt trimming
      if ENABLE_TRIM_HTML_PART && !skip_trim && do_trim
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

    def self.count_topics_in_text_blocks(blocks)
      cutoff = blocks.find_index do |b|
        t = normalize_spaces(b).downcase
        POPULAR_POSTS_MARKERS.any? { |m| t.include?(m) }
      end

      scoped_text = cutoff ? blocks[0...cutoff].join("\n\n") : blocks.join("\n\n")
      ids = scoped_text.scan(%r{/t/(?:[^/\s]+/)?(\d+)}i).flatten.uniq
      ids.size
    rescue
      999 # fail-open: do NOT accidentally skip trimming
    end

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

        b2 = b.to_s.gsub(/\r\n?/, "\n")
        nl = b2.index("\n")
        linebreak_forced = nl && nl > 0 && nl < TEXT_MAX_CHARS

        norm_len = normalize_spaces(b2).length
        need_trim = linebreak_forced || (norm_len > TEXT_MAX_CHARS)
        next unless need_trim

        blocks[i] = smart_trim_plain(b2, TEXT_MAX_CHARS)
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
