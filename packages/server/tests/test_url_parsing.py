import pytest
import seqera


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
    monkeypatch.setattr(seqera, 'find_matching_refs', mock_find_matching_refs)


@pytest.mark.asyncio
async def test_blob_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/talos/blob/main/nextflow.config'
    owner, repo, full_ref, ref, path = await seqera.parse_github_url(url)
    assert owner == 'populationgenomics'
    assert repo == 'talos'
    assert full_ref == 'refs/heads/main'
    assert ref == 'main'
    assert path == '/nextflow.config'


@pytest.mark.asyncio
async def test_refs_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/talos/refs/heads/main/nextflow.config'
    owner, repo, full_ref, ref, path = await seqera.parse_github_url(url)
    assert owner == 'populationgenomics'
    assert repo == 'talos'
    assert full_ref == 'refs/heads/main'
    assert ref == 'main'
    assert path == '/nextflow.config'


@pytest.mark.asyncio
async def test_nested_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/analysis-runner/refs/heads/infra/testing/foo/bar.txt'
    owner, repo, full_ref, ref, path = await seqera.parse_github_url(url)
    assert owner == 'populationgenomics'
    assert repo == 'analysis-runner'
    assert full_ref == 'refs/heads/infra/testing'
    assert ref == 'infra/testing'
    assert path == '/foo/bar.txt'


@pytest.mark.asyncio
async def test_bad_url(mock_gh_api):
    url = 'https://github.com/populationgenomics/analysis-runner/refs/heads/nonexistent/foo.txt'
    with pytest.raises(ValueError, match='No Git reference found'):
        await seqera.parse_github_url(url)
