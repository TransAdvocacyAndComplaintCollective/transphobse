from typing import Optional


class URLItem:
    def __init__(
        self,
        url: str,
        url_score: float = 0.0,
        page_score: float = 0.0,
        page_type: Optional[str] = None,
        lastmod_sitemap: Optional[str] = None,
        changefreq_sitemap: Optional[str] = None,
        priority_sitemap: Optional[float] = None,
        time_last_visited: Optional[str] = None,
        next_revisit_time: Optional[str] = None,
        revisit_count: int = 0,
        flair: Optional[str] = None,
        reddit_score: Optional[float] = None,
        error: Optional[str] = None,
        changefreq: Optional[str] = None,
        lastmod_html: Optional[str] = None,
        priority: Optional[float] = None,
        status: str = "unseen",
        etag: Optional[str] = None,
        last_modified: Optional[str] = None
    ):
        self.url = url
        self.url_score = url_score
        self.page_score = page_score
        self.page_type = page_type
        self.lastmod_sitemap = lastmod_sitemap
        self.changefreq_sitemap = changefreq_sitemap
        self.priority_sitemap = priority_sitemap
        self.time_last_visited = time_last_visited
        self.next_revisit_time = calculate_revisit_time(changefreq, time_last_visited)
        self.revisit_count = revisit_count
        self.flair = flair
        self.reddit_score = reddit_score
        self.error = error
        self.changefreq = changefreq
        self.lastmod_html = lastmod_html
        self.priority = priority
        self.status = status
        self.etag = etag
        self.last_modified = last_modified

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "url_score": self.url_score,
            "page_score": self.page_score,
            "page_type": self.page_type,
            "lastmod_sitemap": self.lastmod_sitemap,
            "changefreq_sitemap": self.changefreq_sitemap,
            "priority_sitemap": self.priority_sitemap,
            "time_last_visited": self.time_last_visited,
            "next_revisit_time": self.next_revisit_time,
            "revisit_count": self.revisit_count,
            "flair": self.flair,
            "reddit_score": self.reddit_score,
            "error": self.error,
            "changefreq": self.changefreq,
            "lastmod": self.lastmod_html,
            "priority": self.priority,
            "status": self.status,
            "etag": self.etag,
            "last_modified": self.last_modified
        }

    @staticmethod
    def from_row(row: aiosqlite.Row) -> 'URLItem':
        return URLItem(
            url=row["url"],
            url_score=row["url_score"],
            page_score=row["page_score"],
            page_type=row["page_type"],
            lastmod_sitemap=row["lastmod_sitemap"],
            changefreq_sitemap=row["changefreq_sitemap"],
            priority_sitemap=row["priority_sitemap"],
            time_last_visited=row["time_last_visited"],
            next_revisit_time=row["next_revisit_time"],
            revisit_count=row["revisit_count"],
            flair=row["flair"],
            reddit_score=row["reddit_score"],
            error=row["error"],
            changefreq=row["changefreq"],
            lastmod_html=row["lastmod"],
            priority=row["priority"],
            status=row["status"],
            etag=row["etag"],
            last_modified=row["last_modified"]
        )
