class PluginBase:
    def on_start(self, crawler):
        """Hook called when the crawler starts."""
        pass

    def before_fetch(self, url, crawler):
        """Hook called before a URL is fetched."""
        return False

    def after_fetch(self, url, content, crawler):
        """Hook called after content is fetched from a URL. Can modify the content."""
        return content

    def on_url_discovered(self, url, url_type, priority, crawler):
        """Hook called when a new URL is discovered."""
        pass

    def on_url_queue_push(self, url_item, crawler):
        """Hook called when a URL is added to the queue."""
        pass

    def on_url_queue_pop(self, url_item, crawler):
        """Hook called when a URL is dequeued for processing."""
        pass

    def on_url_fetch_error(self, url, error, crawler):
        """Hook called when a fetch error occurs."""
        pass

    def on_content_parse_error(self, url, error, crawler):
        """Hook called when an error occurs while parsing content."""
        pass

    def before_process_feed(self, url, feed, crawler):
        """Hook called before a feed is processed."""
        pass

    def before_process_url(self, url, content, crawler):
        """Hook called before a webpage content is processed."""
        pass

    def after_process_url(self, url, metadata, crawler):
        """Hook called after a webpage content is processed."""
        pass

    def on_content_analyzed(self, url, metadata, crawler):
        """Hook called after the content of a page has been analyzed."""
        pass

    def on_csv_updated(self, url, score, found_keywords, metadata):
        """Hook called when the CSV is updated with new data."""
        pass

    def on_url_removed_from_csv(self, url):
        """Hook called when a URL is removed from the CSV."""
        pass

    def on_finish(self, crawler):
        """Hook called when the crawler finishes."""
        pass

    def on_metadata_extracted(self, metadata):
        """Hook called when metadata is extracted. Can modify metadata."""
        return metadata

    def on_crawl_paused(self, crawler):
        """Hook called when the crawl is paused."""
        pass

    def on_crawl_resumed(self, crawler):
        """Hook called when the crawl is resumed."""
        pass
    def by_pass_robots(self,url):
        return False
