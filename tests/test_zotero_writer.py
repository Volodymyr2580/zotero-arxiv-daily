import pytest
from omegaconf import open_dict

from tests.canned_responses import make_sample_paper, make_stub_zotero_client
from zotero_arxiv_daily.zotero_writer import ZoteroInboxWriter


def test_find_collection_key_by_path(config):
    zot = make_stub_zotero_client()
    writer = ZoteroInboxWriter(config, zot)

    assert writer.find_collection_key("00_Inbox/00_to_Read_list") == "TOREAD"


def test_find_collection_key_raises_for_missing_path(config):
    zot = make_stub_zotero_client()
    writer = ZoteroInboxWriter(config, zot)

    with pytest.raises(ValueError, match="Zotero collection path not found"):
        writer.find_collection_key("missing/path")


def test_dry_run_does_not_create_items(config):
    created = []
    zot = make_stub_zotero_client(created_items=created)
    writer = ZoteroInboxWriter(config, zot)
    paper = make_sample_paper(tldr="A concise TLDR.", score=7.25, affiliations=["Lab A"])

    with open_dict(config):
        config.zotero_writer.dry_run = True

    result = writer.write_papers([paper])

    assert result.dry_run is True
    assert created == []
    assert result.items[0]["collections"] == ["TOREAD"]


def test_write_papers_creates_preprint_items(config):
    created = []
    zot = make_stub_zotero_client(created_items=created)
    writer = ZoteroInboxWriter(config, zot)
    paper = make_sample_paper(tldr="A concise TLDR.", score=7.25, affiliations=["Lab A"])

    result = writer.write_papers([paper])

    assert result.dry_run is False
    assert len(created) == 1
    item = created[0]
    assert item["itemType"] == "preprint"
    assert item["title"] == paper.title
    assert item["abstractNote"] == paper.abstract
    assert item["url"] == paper.url
    assert item["collections"] == ["TOREAD"]
    assert item["creators"][0] == {"creatorType": "author", "name": "Author A"}
    assert {"tag": "status/to_read"} in item["tags"]
    assert {"tag": "source/arxiv_daily"} in item["tags"]
    assert "TLDR: A concise TLDR." in item["extra"]
    assert "Relevance Score: 7.2500" in item["extra"]
    assert "PDF URL: https://arxiv.org/pdf/2026.00001" in item["extra"]
