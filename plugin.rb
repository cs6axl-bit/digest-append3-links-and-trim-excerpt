# name: digest-append3-links-and-trim-excerpt
# version: 1.1
# about: Appends isdigest=1, u=<user_id>, dayofweek=<base64url(email)>, email_id=<20-digit> to internal links in Activity Summary (digest) emails.
#        Optimized for high volume: single Nokogiri parse, cheap pre-checks, and separate switches for HTML/TEXT trimming.
#        HTML trim now preserves existing markup/spacing by trimming text nodes in-place (no wiping children).

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

    def self.normalize_spaces(s)
      s.to_s.gsub(/\r\n?/, "\n").gsub(/[ \t]+/, " ").strip
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

    # ============================================================
    # HTML: trim in-place by editing text nodes (preserves <p> spacing)
    # ============================================================

    def self.trim_html_node_in_place!(node, max_chars)
      # total visible text
      full = node.text.to_s
      full_norm = normalize_spaces(full)
      return false if full_norm.length <= max_chars

      # Gather ALL text nodes under this node (excluding script/style)
      text_nodes = node.xpath(".//text()[not(ancestor::script) and not(ancestor::style)]").to_a
      return false if text_nodes.empty?

      # We trim from the end backwards until within limit
      remaining = full_norm.length

      # To avoid expensive re-normalization each time, we approximate per-node lengths by normalized text.
      # This is fine for email excerpts (mostly plain).
      text_nodes.reverse_each do |tn|
        t = tn.text.to_s
        t_norm = normalize_spaces(t)
        next if t_norm.empty?

        if remaining <= max_chars
          break
        end

        over = remaining - max_chars

        if t_norm.length <= over
          # remove this text node entirely
          tn.remove
          remaining -= t_norm.length
        else
          # shorten this node and finish
          new_text = smart_trim_plain(t_norm, t_norm.length - over)
          # Ensure ellipsis only once at the very end
          new_text = new_text.sub(/…+$/, "") # strip existing ellipsis from smart trim
          new_text = smart_trim_plain(t_norm, (t_norm.length - over))
          # smart_trim_plain already adds "…"
          tn.content = new_text
          remaining = max_chars
          break
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

      # Cheap skip if no work
      if ENABLE_LINK_REWRITE && !body.include?('href="') && !body.include?("href='")
        return unless ENABLE_TRIM_HTML_PART
      end

      if ENABLE_TRIM_HTML_PART
        selector_hints = ["digest-post-excerpt", "post-excerpt", "topic-excerpt", "itemprop=\"articleBody\"", "itemprop='articleBody'", "excerpt"]
        has_trim_hint = selector_hints.any? { |h| body.include?(h) }
        return if !has_trim_hint && !ENABLE_LINK_REWRITE
      end

      if !Nokogiri
        # Regex rewrite only; skip HTML trimming
        if ENABLE_LINK_REWRITE
          html_part.body = rewrite_links_regex(body, user, email_id, base)
        end
        return
      end

      dayofweek_val = encoded_email(user)
      doc = Nokogiri::HTML(body)
      changed = false

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

      if ENABLE_TRIM_HTML_PART
        nodes =
          HTML_EXCERPT_SELECTORS
            .flat_map { |sel| doc.css(sel).to_a }
            .uniq

        nodes.each do |node|
          # Never trim if node contains unsubscribe/preferences links
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

    def self.trim_digest_text_part!(message)
      return if message.nil?
      return unless ENABLE_TRIM_TEXT_PART
      return unless message.respond_to?(:text_part) && message.text_part

      tp = message.text_part
      text = tp.body&.decoded
      return if text.nil? || text.empty?

      t = text.to_s.gsub(/\r\n?/, "\n")
      blocks = t.split(/\n{2,}/)

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
