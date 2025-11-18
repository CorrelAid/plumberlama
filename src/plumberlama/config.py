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
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        processed_data_output_path: str = None,
    ):
        # Validate required inputs
        assert lp_poll_id > 0, "poll_id must be positive"
        assert lp_api_token, "lama_api_token must not be empty"
        assert lp_api_base_url, "base_url must not be empty"
        assert db_host, "db_host must not be empty"

        self.survey_id = survey_id

        # LamaPoll API configuration
        self.lp_poll_id = lp_poll_id
        self.lp_api_token = lp_api_token
        self.lp_api_base_url = lp_api_base_url

        # LLM configuration (optional - only needed for variable naming)
        self.llm_base_url = llm_base_url
        self.llm_model = llm_model
        self.llm_key = llm_key

        # Processed data output configuration (optional - for saving before anonymization)
        self.processed_data_output_path = processed_data_output_path

        # Database configuration
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def get_db_connection_uri(self) -> str:
        """Get PostgreSQL connection URI from config.

        Returns:
            Connection URI string for PostgreSQL
        """
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
