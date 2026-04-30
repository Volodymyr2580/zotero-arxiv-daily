from dataclasses import dataclass
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
        items = [self.paper_to_item(paper, collection_key) for paper in papers]

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
