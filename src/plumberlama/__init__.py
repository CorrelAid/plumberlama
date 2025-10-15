from plumberlama.main import run_etl_pipeline


class Config:
    """Pipeline configuration (immutable by convention)."""

    def __init__(
        self,
        survey_id: str,
        lp_poll_id: int,
        lp_api_token: str,
        lp_api_base_url: str,
        llm_model: str,
        llm_key: str,
        llm_base_url: str,
        doc_output_dir: str,
        mkdocs_site_name: str = None,
        mkdocs_site_author: str = None,
        mkdocs_repo_url: str = None,
        mkdocs_logo_url: str = None,
    ):
        # Validate inputs
        assert lp_poll_id > 0, "poll_id must be positive"
        assert lp_api_token, "lama_api_token must not be empty"
        assert lp_api_base_url, "base_url must not be empty"
        assert llm_key, "llm_key must not be empty"

        self.survey_id = survey_id

        self.lp_poll_id = lp_poll_id
        self.lp_api_token = lp_api_token
        self.lp_api_base_url = lp_api_base_url

        self.llm_base_url = llm_base_url
        self.llm_model = llm_model
        self.llm_key = llm_key

        self.doc_output_dir = doc_output_dir

        # MkDocs configuration
        self.mkdocs_site_name = mkdocs_site_name or f"{survey_id} Survey Documentation"
        self.mkdocs_site_author = mkdocs_site_author or "Survey Team"
        self.mkdocs_repo_url = mkdocs_repo_url
        self.mkdocs_logo_url = (
            mkdocs_logo_url or "https://civic-data.de/app/themes/cdl/img/cdl-logo.svg"
        )


__all__ = ["Config", "run_etl_pipeline"]
