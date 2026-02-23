# frozen_string_literal: true

# name: digest-append3-links-and-trim-excerpt
# version: 1.8.6
# about: Appends isdigest=1, u=<user_id>, dayofweek=<base64url(email)>, email_id=<20-digit> to internal links in Activity Summary (digest) emails.
#        PLUS (v1.4): Rewrites ALL links inside post excerpt bodies to /content?u=<base64url(final_url)> so email clients show only local links.
#        PLUS (v1.5): If there is only one p.digest-topic-name in HTML, skip excerpt trimming entirely (fast + accurate).
#        FIX  (v1.6): Topic counting computed BEFORE /content rewrites; Popular Posts boundary uses document order (robust even if whole email is one big table).
#        PLUS (v1.7): Trim to MIN(max_chars, first visual line break).
#        FIX  (v1.7.1): Line-break trimming now RESPECTS ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES (keep images/objects when false).
#        PLUS (v1.7.6): For /content rewrite (post-body links), append aff_sub2 and subid2 where value is "#{user_id}-#{topic_id_of_current_digest_topic}" (topic id derived from surrounding digest HTML, not from destination URL).
#        PLUS (v1.7.7): Tracking value becomes "#{user_id}-#{topic_id_context}-#{email_id}" and ALSO appends email_id as a separate param on the FINAL destination URL before encoding.
#        FIX  (v1.7.8): Normalizes bad affiliate URLs where query params are mistakenly placed in the PATH like "/&subid=..." before appending our tracking params.
#        CHANGE (v1.7.9): For post-body link tracking params (aff_sub2/subid2), use "-" separators instead of "," (userid-topicid-emailid).
#        PLUS (v1.8.0): FINAL PASS: replace origin domain -> target domain on ALL links (including unsubscribe), controlled by plugin settings.
#        PLUS (v1.8.2): Supports MULTIPLE target domains (comma/newline/space separated) and picks one randomly per email.
#        FIX  (v1.8.3): Domain swapping can apply to TEXT part as well (plain-text URLs).
#        PLUS (v1.8.6): Separate switches for:
#          - HTML <a href> links
#          - TEXT links
#          - HTML "everywhere" (images/scripts/css/etc) (independent of links)
#          - headers (List-Unsubscribe etc.)
#          - Message-ID header domain

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
    # CONFIG (existing behavior)
    # ============================================================

    ENABLE_LINK_REWRITE = true

    ENABLE_CONTENT_REDIRECTOR_FOR_POST_BODY_LINKS = true
    CONTENT_REDIRECTOR_PATH  = "/content"
    CONTENT_REDIRECTOR_PARAM = "u"

    ENABLE_APPEND_TRACKING_PARAMS_TO_POST_BODY_LINKS = true
    TRACKING_PARAMS_TO_APPEND = ["aff_sub2", "subid2"]

    ENABLE_APPEND_EMAIL_ID_TO_POST_BODY_LINKS = true
    POST_BODY_EMAIL_ID_PARAM = "email_id"

    ENABLE_TRIM_HTML_PART = true
    HTML_MAX_CHARS        = 300

    ENABLE_TRIM_TEXT_PART = true
    TEXT_MAX_CHARS        = 300

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

        # Pick ONE target per email only if master enabled and targets exist
        picked_target =
          if ::DigestAppendData.setting_enable_domain_swap
            ::DigestAppendData.pick_target_domain(::DigestAppendData.setting_target_domains)
          else
            ""
          end

        ::DigestAppendData.process_html_part!(message, user, email_id, picked_target)
        ::DigestAppendData.trim_digest_text_part!(message)

        # Domain swap passes (each has its own switch, all gated by master enable)
        if ::DigestAppendData.setting_enable_domain_swap
          origin = ::DigestAppendData.setting_origin_domain

          if ::DigestAppendData.setting_swap_text_links
            ::DigestAppendData.final_swap_domains_in_text_part!(message, origin, picked_target)
          end

          if ::DigestAppendData.setting_swap_headers
            ::DigestAppendData.final_swap_domains_in_headers!(message, origin, picked_target)
          end

          if ::DigestAppendData.setting_swap_message_id
            ::DigestAppendData.final_swap_domain_in_message_id!(message, origin, picked_target)
          end
        end
      end
    end

    # ============================================================
    # Basics
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
      s.to_s.gsub(/\s+/, " ").strip
    end

    def self.contains_any?(haystack, needles)
      h = haystack.to_s
      needles.any? { |n| h.include?(n) }
    end

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

    def self.topic_id_context_for_excerpt(node, base)
      return nil unless node

      begin
        anc = node.at_xpath("ancestor-or-self::*[@data-topic-id or @data-topicid or @topic-id][1]")
        if anc
          v = anc["data-topic-id"] || anc["data-topicid"] || anc["topic-id"]
          vv = v.to_s.strip
          return vv if vv.match?(/^\d+$/)
        end
      rescue
      end

      begin
        a = node.at_xpath("preceding::p[contains(concat(' ', normalize-space(@class), ' '), ' digest-topic-name ')][1]//a[@href][1]")
        if a && a["href"]
          id = extract_topic_id_from_href(a["href"], base)
          return id if id
        end
      rescue
      end

      begin
        a2 = node.at_xpath("preceding::a[contains(@href, '/t/')][1]")
        if a2 && a2["href"]
          id2 = extract_topic_id_from_href(a2["href"], base)
          return id2 if id2
        end
      rescue
      end

      nil
    end

    def self.user_topic_email_value(user_id, topic_id_context, email_id)
      uid = user_id.to_s
      return "" if uid.empty?

      tid = topic_id_context.to_s
      eid = email_id.to_s
      return "" if eid.empty?

      "#{uid}-#{tid}-#{eid}"
    end

    # ============================================================
    # URL normalization for broken affiliate URLs
    # ============================================================

    def self.normalize_query_stuck_in_path!(uri)
      return false unless uri
      return false if uri.query && !uri.query.to_s.empty?

      path = uri.path.to_s
      return false if path.empty?

      m = path.match(%r{^(.*?)/&([^#?]+)$})
      return false unless m

      tail = m[2].to_s
      return false unless tail.include?("=")

      uri.path = m[1].to_s + "/"
      uri.query = tail
      true
    rescue
      false
    end

    # ============================================================
    # Append tracking params (post-body links) BEFORE encoding into /content
    # ============================================================

    def self.append_tracking_params(url, user_id, topic_id_context, email_id)
      return url if url.to_s.empty?

      uri = URI.parse(url)
      return url unless uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)

      normalize_query_stuck_in_path!(uri)

      params = URI.decode_www_form(uri.query || "")

      if ENABLE_APPEND_EMAIL_ID_TO_POST_BODY_LINKS
        k = POST_BODY_EMAIL_ID_PARAM.to_s
        if !k.empty? && email_id.to_s != "" && !params.any? { |kk, _| kk == k }
          params << [k, email_id.to_s]
        end
      end

      val = user_topic_email_value(user_id, topic_id_context, email_id)

      if val.empty?
        uri.query = URI.encode_www_form(params)
        return uri.to_s
      end

      TRACKING_PARAMS_TO_APPEND.each do |k|
        kk = k.to_s
        next if kk.empty?
        next if params.any? { |k2, _| k2 == kk }
        params << [kk, val]
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

    def self.primary_topic_count_before_popular(doc)
      return nil unless doc
      base   = Discourse.base_url
      marker = find_popular_marker_node(doc)

      keys = []

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

    def self.trim_html_at_first_line_break!(node, max_chars)
      return false unless node

      br = node.at_css("br")
      if br
        before_len = normalize_spaces(text_before_node(node, br)).length
        if before_len > 0 && before_len < max_chars && has_content_after_boundary?(br, node)
          if ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES
            remove_following_siblings_up_to_root!(br, node)
          else
            remove_text_nodes_after_end!(node, br)
          end
          br.remove
          return true
        end
      end

      boundary = node.at_css("p,li")
      if boundary
        kept_len = normalize_spaces(boundary.text).length
        if kept_len > 0 && kept_len < max_chars && has_content_after_boundary?(boundary, node)
          if ENABLE_TRIM_HTML_REMOVE_TRAILING_NODES
            remove_following_siblings_up_to_root!(boundary, node)
          else
            end_node = end_node_for_kept_region(boundary)
            remove_text_nodes_after_end!(node, end_node)
          end
          return true
        end
      end

      false
    rescue
      false
    end

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
    # Domain swap settings (master + per-area switches)
    # ============================================================

    def self.setting_enable_domain_swap
      v = (defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_enable_domain_swap)) ? SiteSetting.digest_append_enable_domain_swap : false
      !!v
    rescue
      false
    end

    def self.setting_origin_domain
      v = (defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_origin_domain)) ? SiteSetting.digest_append_origin_domain : ""
      v.to_s.strip
    rescue
      ""
    end

    def self.setting_target_domains
      raw =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_target_domains)
          SiteSetting.digest_append_target_domains
        else
          ""
        end

      raw.to_s
         .split(/[\s,]+/)
         .map { |x| x.to_s.strip }
         .reject(&:empty?)
    rescue
      []
    end

    # Switches you asked for
    def self.setting_swap_html_links
      v =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_domain_swap_html_links)
          SiteSetting.digest_append_domain_swap_html_links
        else
          true
        end
      !!v
    rescue
      true
    end

    def self.setting_swap_text_links
      v =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_domain_swap_text_links)
          SiteSetting.digest_append_domain_swap_text_links
        else
          false
        end
      !!v
    rescue
      false
    end

    # “everywhere (not just links)” = swap HTML resource URL attributes (img/src, script/src, link[href], srcset, etc)
    def self.setting_swap_everywhere
      v =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_domain_swap_everywhere)
          SiteSetting.digest_append_domain_swap_everywhere
        else
          false
        end
      !!v
    rescue
      false
    end

    def self.setting_swap_headers
      v =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_domain_swap_headers)
          SiteSetting.digest_append_domain_swap_headers
        else
          false
        end
      !!v
    rescue
      false
    end

    def self.setting_swap_message_id
      v =
        if defined?(SiteSetting) && SiteSetting.respond_to?(:digest_append_domain_swap_message_id)
          SiteSetting.digest_append_domain_swap_message_id
        else
          false
        end
      !!v
    rescue
      false
    end

    def self.pick_target_domain(targets)
      return "" if targets.nil? || targets.empty?
      targets[SecureRandom.random_number(targets.size)].to_s
    rescue
      targets.first.to_s
    end

    def self.normalize_host_only(s)
      x = s.to_s.strip
      return "" if x.empty?
      x = x.sub(%r{\Ahttps?://}i, "")
      x = x.split("/").first.to_s
      x = x.split("?").first.to_s
      x = x.split("#").first.to_s
      x.downcase
    rescue
      ""
    end

    # If host == origin OR host ends with ".origin", replace just the origin suffix, preserving subdomain prefix.
    def self.rewrite_host_if_matches(host, origin, target)
      h = host.to_s
      return nil if h.empty?

      o = normalize_host_only(origin)
      t = normalize_host_only(target)
      return nil if o.empty? || t.empty?
      return nil if normalize_host_only(h) == t

      h_lc = h.downcase
      if h_lc == o
        return target.to_s.strip
      end

      suffix = ".#{o}"
      if h_lc.end_with?(suffix)
        prefix = h[0, h.length - suffix.length]
        return "#{prefix}.#{target.to_s.strip}"
      end

      nil
    rescue
      nil
    end

    # ============================================================
    # Domain swap: HTML URLs (links and/or resource attributes)
    # ============================================================

    def self.rewrite_single_url_string(url_str, base, origin_domain, target_domain_for_email)
      u = url_str.to_s.strip
      return nil if u.empty?
      return nil if u.start_with?("mailto:", "tel:", "sms:", "#")

      abs = u.start_with?("/") ? (base.to_s + u) : u

      begin
        uri = URI.parse(abs)
      rescue
        return nil
      end

      return nil unless uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)
      return nil if uri.host.to_s.empty?

      new_host = rewrite_host_if_matches(uri.host, origin_domain, target_domain_for_email)
      return nil unless new_host

      uri.host = new_host
      uri.to_s
    rescue
      nil
    end

    def self.rewrite_srcset_value(srcset, base, origin_domain, target_domain_for_email)
      raw = srcset.to_s
      return nil if raw.strip.empty?

      parts = raw.split(",").map(&:strip).reject(&:empty?)
      return nil if parts.empty?

      changed = false
      new_parts = parts.map do |p|
        tokens = p.split(/\s+/, 2)
        url = tokens[0].to_s
        rest = tokens.length > 1 ? tokens[1].to_s : ""

        rewritten = rewrite_single_url_string(url, base, origin_domain, target_domain_for_email)
        if rewritten
          changed = true
          rest.empty? ? rewritten : "#{rewritten} #{rest}"
        else
          p
        end
      end

      changed ? new_parts.join(", ") : nil
    rescue
      nil
    end

    def self.swap_html_links_only!(doc, base, origin_domain, target_domain_for_email)
      return false unless doc
      return false if origin_domain.to_s.strip.empty? || target_domain_for_email.to_s.strip.empty?

      changed = false
      doc.css("a[href]").each do |a|
        href = a["href"].to_s
        new_url = rewrite_single_url_string(href, base, origin_domain, target_domain_for_email)
        next unless new_url
        a["href"] = new_url
        changed = true
      end
      changed
    rescue
      false
    end

    # "Everywhere" here means resource attributes (NOT dependent on link switch)
    def self.swap_html_resource_attributes_everywhere!(doc, base, origin_domain, target_domain_for_email)
      return false unless doc
      return false if origin_domain.to_s.strip.empty? || target_domain_for_email.to_s.strip.empty?

      changed = false

      attr_selectors = [
        ["img[src]", "src"],
        ["img[data-src]", "data-src"],
        ["source[src]", "src"],
        ["video[src]", "src"],
        ["audio[src]", "src"],
        ["iframe[src]", "src"],
        ["script[src]", "src"],
        ["link[href]", "href"],   # css/icons etc. (not <a>)
        ["form[action]", "action"],
        ["video[poster]", "poster"]
      ]

      attr_selectors.each do |sel, attr|
        doc.css(sel).each do |node|
          v = node[attr].to_s
          next if v.strip.empty?
          new_url = rewrite_single_url_string(v, base, origin_domain, target_domain_for_email)
          next unless new_url
          node[attr] = new_url
          changed = true
        end
      end

      doc.css("img[srcset],source[srcset]").each do |node|
        v = node["srcset"].to_s
        next if v.strip.empty?
        new_srcset = rewrite_srcset_value(v, base, origin_domain, target_domain_for_email)
        next unless new_srcset
        node["srcset"] = new_srcset
        changed = true
      end

      changed
    rescue
      false
    end

    # ============================================================
    # Domain swap: TEXT links
    # ============================================================

    TEXT_URL_REGEX = %r{https?://[^\s<>"'()]+}i

    def self.strip_trailing_url_punct(url)
      u = url.to_s
      return [u, ""] if u.empty?

      suffix = +""
      while u.length > 0 && u[-1].match?(/[)\].,;:!?]/)
        suffix.prepend(u[-1])
        u = u[0..-2]
      end

      [u, suffix]
    rescue
      [url.to_s, ""]
    end

    def self.swap_domain_in_single_text_url(url, origin_domain, target_domain_for_email)
      core, suffix = strip_trailing_url_punct(url)

      begin
        uri = URI.parse(core)
      rescue
        return url
      end

      return url unless uri.is_a?(URI::HTTP) || uri.is_a?(URI::HTTPS)
      return url if uri.host.to_s.empty?

      new_host = rewrite_host_if_matches(uri.host, origin_domain, target_domain_for_email)
      return url unless new_host

      uri.host = new_host
      uri.to_s + suffix
    rescue
      url
    end

    def self.final_swap_domains_in_text_part!(message, origin_domain, target_domain_for_email)
      return false unless message
      return false if origin_domain.to_s.strip.empty? || target_domain_for_email.to_s.strip.empty?
      return false unless message.respond_to?(:text_part) && message.text_part

      tp = message.text_part
      text = tp.body&.decoded
      return false if text.nil? || text.empty?

      changed = false
      out = text.to_s.gsub(TEXT_URL_REGEX) do |found|
        swapped = swap_domain_in_single_text_url(found, origin_domain, target_domain_for_email)
        changed = true if swapped != found
        swapped
      end

      tp.body = out if changed
      changed
    rescue => e
      Rails.logger.warn("digest-append3-links-and-trim-excerpt TEXT domain-swap failed: #{e.class}: #{e.message}")
      false
    end

    # ============================================================
    # Domain swap: HEADERS (URL-ish headers)
    # ============================================================

    HEADERS_TO_DOMAIN_SWAP = [
      "List-Unsubscribe",
      "List-Help",
      "List-Subscribe",
      "List-Owner"
    ].freeze

    def self.swap_domains_in_header_value(value, origin_domain, target_domain_for_email)
      s = value.to_s
      return s if s.empty?

      s.gsub(TEXT_URL_REGEX) do |found|
        swap_domain_in_single_text_url(found, origin_domain, target_domain_for_email)
      end
    rescue
      value.to_s
    end

    def self.final_swap_domains_in_headers!(message, origin_domain, target_domain_for_email)
      return false unless message
      return false if origin_domain.to_s.strip.empty? || target_domain_for_email.to_s.strip.empty?

      changed = false

      HEADERS_TO_DOMAIN_SWAP.each do |hname|
        begin
          hdr = message.header[hname]
          next unless hdr

          old_val = hdr.to_s
          next if old_val.to_s.strip.empty?

          new_val = swap_domains_in_header_value(old_val, origin_domain, target_domain_for_email)
          next if new_val == old_val

          message.header[hname] = new_val
          changed = true
        rescue
          next
        end
      end

      changed
    rescue
      false
    end

    # ============================================================
    # Domain swap: Message-ID header (domain after @ inside <...>)
    # ============================================================

    def self.swap_message_id_value(value, origin_domain, target_domain_for_email)
      s = value.to_s.strip
      return s if s.empty?

      # Typical: <uuid@myhealthyhaven.org>
      m = s.match(/<([^<>@\s]+)@([^<>@\s]+)>/)
      return s unless m

      local = m[1].to_s
      host  = m[2].to_s
      return s if local.empty? || host.empty?

      new_host = rewrite_host_if_matches(host, origin_domain, target_domain_for_email)
      return s unless new_host

      s.sub(/<#{Regexp.escape(local)}@#{Regexp.escape(host)}>/, "<#{local}@#{new_host}>")
    rescue
      value.to_s
    end

    def self.final_swap_domain_in_message_id!(message, origin_domain, target_domain_for_email)
      return false unless message
      return false if origin_domain.to_s.strip.empty? || target_domain_for_email.to_s.strip.empty?

      begin
        hdr = message.header["Message-ID"]
        return false unless hdr

        old_val = hdr.to_s
        return false if old_val.to_s.strip.empty?

        new_val = swap_message_id_value(old_val, origin_domain, target_domain_for_email)
        return false if new_val == old_val

        message.header["Message-ID"] = new_val
        true
      rescue
        false
      end
    end

    # ============================================================
    # HTML processing (single Nokogiri pass)
    # ============================================================

    def self.process_html_part!(message, user, email_id, target_domain_for_email = "")
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

      primary_topic_count = primary_topic_count_before_popular(doc)
      skip_trim = (primary_topic_count == 1)
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
          topic_ctx = topic_id_context_for_excerpt(node, base)

          node.css("a[href]").each do |a|
            href = a["href"].to_s.strip
            next if href.empty?
            next if href.start_with?("mailto:", "tel:", "sms:", "#")
            next if contains_any?(href, NEVER_TOUCH_HREF_SUBSTRINGS)

            abs0 = absolute_url_from_href(href, base)
            next if abs0.nil?
            next unless http_url?(abs0)

            begin
              u0 = URI.parse(abs0)
              b0 = URI.parse(base)
              if u0.host == b0.host && u0.path == CONTENT_REDIRECTOR_PATH
                next
              end
            rescue
            end

            final_dest =
              if ENABLE_APPEND_TRACKING_PARAMS_TO_POST_BODY_LINKS
                append_tracking_params(abs0, user.id, topic_ctx, email_id)
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

      # 4) FINAL PASS: domain swap in HTML (switches)
      if setting_enable_domain_swap && !target_domain_for_email.to_s.strip.empty?
        origin = setting_origin_domain
        picked = target_domain_for_email.to_s

        # HTML links (<a href>)
        if setting_swap_html_links
          changed = true if swap_html_links_only!(doc, base, origin, picked)
        end

        # HTML "everywhere" (resource attributes)
        if setting_swap_everywhere
          changed = true if swap_html_resource_attributes_everywhere!(doc, base, origin, picked)
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
      999
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
