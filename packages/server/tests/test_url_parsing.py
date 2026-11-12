import pytest
import util
from aiohttp import web


async def mock_find_matching_refs(
    owner: str, repo: str, ref_kinds: list[str], prefix: str
) -> list[tuple[str, str]]:
    if 'heads' not in ref_kinds or prefix == 'nonexistent':
        return []

    if prefix == 'main':
        return [('refs/heads/main', 'main')]

    if prefix == 'infra':
        return [
            ('refs/heads/infrastructure', 'infrastructure'),
            ('refs/heads/infra/foo', 'infra/foo'),
            ('refs/heads/infra/testing', 'infra/testing'),
        ]

    raise ValueError(f'Unexpected {prefix=}')


@pytest.fixture
def mock_gh_api(monkeypatch):
    monkeypatch.setattr(util, 'find_matching_refs', mock_find_matching_refs)


@pytest.mark.asyncio
async def test_blob_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/talos/blob/main/nextflow.config'
    gh_url = await util.GithubUrl.parse(url)
    await gh_url.parse_path()
    assert gh_url.owner == 'populationgenomics'
    assert gh_url.repo == 'talos'
    assert gh_url.full_ref == 'refs/heads/main'
    assert gh_url.ref == 'main'
    assert gh_url.path == '/nextflow.config'


@pytest.mark.asyncio
async def test_refs_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/talos/refs/heads/main/nextflow.config'
    gh_url = await util.GithubUrl.parse(url)
    await gh_url.parse_path()
    assert gh_url.owner == 'populationgenomics'
    assert gh_url.repo == 'talos'
    assert gh_url.full_ref == 'refs/heads/main'
    assert gh_url.ref == 'main'
    assert gh_url.path == '/nextflow.config'


@pytest.mark.asyncio
async def test_nested_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/analysis-runner/refs/heads/infra/testing/foo/bar.txt'
    gh_url = await util.GithubUrl.parse(url)
    await gh_url.parse_path()
    assert gh_url.owner == 'populationgenomics'
    assert gh_url.repo == 'analysis-runner'
    assert gh_url.full_ref == 'refs/heads/infra/testing'
    assert gh_url.ref == 'infra/testing'
    assert gh_url.path == '/foo/bar.txt'


@pytest.mark.asyncio
async def test_malicious_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/talos/blob/main/../../../../evil/repo/contents/evil.config'
    with pytest.raises(web.HTTPBadRequest):
        await util.GithubUrl.parse(url)


@pytest.mark.asyncio
async def test_bad_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/analysis-runner/refs/heads/nonexistent/foo.txt'
    with pytest.raises(ValueError, match='No Git reference found'):
        await (await util.GithubUrl.parse(url)).parse_path()
