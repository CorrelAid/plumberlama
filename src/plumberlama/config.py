class Config:
    """Pipeline configuration (immutable by convention)."""

    def __init__(
        self,
        survey_id: str,
        lp_poll_id: int,
        lp_api_token: str,
        lp_api_base_url: str,
        llm_model: str = None,
        llm_key: str = None,
        llm_base_url: str = None,
        doc_output_dir: str = None,
        mkdocs_site_name: str = None,
        mkdocs_site_author: str = None,
        mkdocs_repo_url: str = None,
        mkdocs_logo_url: str = None,
        db_host: str = None,
        db_port: int = None,
        db_name: str = None,
        db_user: str = None,
        db_password: str = None,
    ):
        # Validate required inputs
        assert lp_poll_id > 0, "poll_id must be positive"
        assert lp_api_token, "lama_api_token must not be empty"
        assert lp_api_base_url, "base_url must not be empty"

        self.survey_id = survey_id

        # LamaPoll API configuration
        self.lp_poll_id = lp_poll_id
        self.lp_api_token = lp_api_token
        self.lp_api_base_url = lp_api_base_url

        # LLM configuration (optional - only needed for variable naming)
        self.llm_base_url = llm_base_url
        self.llm_model = llm_model
        self.llm_key = llm_key

        # Documentation configuration
        self.doc_output_dir = doc_output_dir or "/tmp/docs"

        # MkDocs configuration
        self.mkdocs_site_name = mkdocs_site_name or f"{survey_id} Survey Documentation"
        self.mkdocs_site_author = mkdocs_site_author or "Survey Team"
        self.mkdocs_repo_url = mkdocs_repo_url
        self.mkdocs_logo_url = (
            mkdocs_logo_url or "https://civic-data.de/app/themes/cdl/img/cdl-logo.svg"
        )

        # Database configuration
        self.db_host = db_host or "localhost"
        self.db_port = db_port or 5432
        self.db_name = db_name or "survey_data"
        self.db_user = db_user or "plumberlama"
        self.db_password = db_password or "plumberlama_dev"

    def get_db_connection_uri(self) -> str:
        """Get PostgreSQL connection URI from config.

        Returns:
            Connection URI string for PostgreSQL
        """
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
