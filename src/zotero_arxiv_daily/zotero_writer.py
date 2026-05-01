from dataclasses import dataclass
import re
from typing import Any

from loguru import logger
from omegaconf import DictConfig

from .protocol import Paper


@dataclass
class ZoteroWriteResult:
    dry_run: bool
    collection_key: str
    items: list[dict[str, Any]]
    response: Any = None


class ZoteroInboxWriter:
    def __init__(self, config: DictConfig, zotero_client: Any):
        self.config = config
        self.zotero = zotero_client
        self.writer_config = config.zotero_writer

    def write_papers(self, papers: list[Paper]) -> ZoteroWriteResult:
        collection_path = self.writer_config.collection_path
        collection_key = self.find_collection_key(collection_path)
        papers_to_write = self.filter_existing_papers(papers)
        items = [self.paper_to_item(paper, collection_key) for paper in papers_to_write]

        if self.writer_config.dry_run:
            logger.info(f"Dry run: {len(items)} Zotero items would be created in {collection_path}")
            for item in items:
                logger.info(f"Would create Zotero item: {item['title']}")
            return ZoteroWriteResult(True, collection_key, items)

        if not items:
            logger.info("No Zotero items to create.")
            return ZoteroWriteResult(False, collection_key, items, response={"success": {}, "failed": {}, "unchanged": {}})

        logger.info(f"Creating {len(items)} Zotero items in {collection_path}")
        checked_items = self.zotero.check_items(items)
        response = self.zotero.create_items(checked_items)
        logger.info(f"Zotero create_items response: {response}")
        return ZoteroWriteResult(False, collection_key, checked_items, response)

    def filter_existing_papers(self, papers: list[Paper]) -> list[Paper]:
        if not self.writer_config.get("skip_existing", True):
            return papers

        existing_identifiers = self.fetch_existing_identifiers()
        filtered: list[Paper] = []
        skipped = 0
        for paper in papers:
            identifiers = self.paper_identifiers(paper)
            if existing_identifiers.intersection(identifiers):
                skipped += 1
                logger.info(f"Skipping existing Zotero paper: {paper.title}")
                continue
            filtered.append(paper)

        if skipped:
            logger.info(f"Skipped {skipped} papers already present in Zotero.")
        return filtered

    def fetch_existing_identifiers(self) -> set[str]:
        items = self.zotero.everything(self.zotero.items(itemType="conferencePaper || journalArticle || preprint"))
        identifiers: set[str] = set()
        for item in items:
            data = item.get("data", item)
            identifiers.update(
                self.text_identifiers(
                    str(data.get("title") or ""),
                    str(data.get("url") or ""),
                    str(data.get("DOI") or data.get("doi") or ""),
                    str(data.get("extra") or ""),
                )
            )
        return identifiers

    def paper_identifiers(self, paper: Paper) -> set[str]:
        return self.text_identifiers(paper.title, paper.url, "", paper.pdf_url or "")

    def text_identifiers(self, title: str, url: str, doi: str, extra: str) -> set[str]:
        identifiers: set[str] = set()
        normalized_title = " ".join(title.lower().split())
        if normalized_title:
            identifiers.add(f"title:{normalized_title}")

        for text in [url, doi, extra]:
            if not text:
                continue
            arxiv_id = self.extract_arxiv_id(text)
            if arxiv_id:
                identifiers.add(f"arxiv:{arxiv_id}")
            normalized_text = text.strip().lower()
            if normalized_text:
                identifiers.add(f"text:{normalized_text}")
        return identifiers

    def extract_arxiv_id(self, text: str) -> str | None:
        match = re.search(r"arxiv\.org/(?:abs|pdf)/([0-9]{4}\.[0-9]{4,5})(?:v\d+)?", text)
        if match:
            return match.group(1)
        match = re.search(r"\barXiv:([0-9]{4}\.[0-9]{4,5})(?:v\d+)?", text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def find_collection_key(self, target_path: str) -> str:
        collections = self.zotero.everything(self.zotero.collections())
        by_key = {collection["key"]: collection for collection in collections}
        path_to_keys: dict[str, list[str]] = {}

        def collection_path(collection_key: str) -> str:
            collection = by_key[collection_key]
            parent = collection["data"].get("parentCollection")
            name = collection["data"]["name"]
            if parent:
                return collection_path(parent) + "/" + name
            return name

        for collection in collections:
            path = collection_path(collection["key"])
            path_to_keys.setdefault(path, []).append(collection["key"])

        matches = path_to_keys.get(target_path, [])
        if not matches:
            raise ValueError(f"Zotero collection path not found: {target_path}")
        if len(matches) > 1:
            raise ValueError(f"Zotero collection path is ambiguous: {target_path}")
        return matches[0]

    def paper_to_item(self, paper: Paper, collection_key: str) -> dict[str, Any]:
        item = self.zotero.item_template("preprint")
        item["title"] = paper.title
        item["creators"] = [{"creatorType": "author", "name": author} for author in paper.authors]
        item["abstractNote"] = paper.abstract or ""
        item["url"] = paper.url
        item["collections"] = [collection_key]
        item["tags"] = [
            {"tag": self.writer_config.tags.status},
            {"tag": f"source/{paper.source}_daily"},
        ]
        item["extra"] = self.format_extra(paper)
        return item

    def format_extra(self, paper: Paper) -> str:
        lines = [
            "PaperFlow Agent",
            f"Source: {paper.source}",
        ]
        if paper.score is not None:
            lines.append(f"Relevance Score: {paper.score:.4f}")
        if paper.pdf_url:
            lines.append(f"PDF URL: {paper.pdf_url}")
        if paper.tldr:
            lines.append(f"TLDR: {paper.tldr}")
        if paper.affiliations:
            lines.append("Affiliations: " + ", ".join(paper.affiliations))
        return "\n".join(lines)
