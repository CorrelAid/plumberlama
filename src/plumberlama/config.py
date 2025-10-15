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
        mkdocs_site_name: str,
        mkdocs_site_author: str,
        mkdocs_repo_url: str,
        mkdocs_logo_url: str,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
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


def build_mkdoc_config(docs_path, site_dir, site_name, site_author, logo_url):
    return {
        "site_name": site_name,
        "site_description": f"Documentation for {site_name}",
        "site_author": site_author,
        "theme": {
            "name": "material",
            "logo": logo_url,
            "features": [
                "navigation.instant",
                "navigation.tracking",
                "navigation.tabs",
                "navigation.sections",
                "navigation.expand",
                "navigation.top",
                "search.suggest",
                "search.highlight",
                "search.share",
                "toc.follow",
                "content.code.copy",
            ],
            "palette": [
                {
                    "media": "(prefers-color-scheme: light)",
                    "scheme": "default",
                    "primary": "custom",
                    "accent": "custom",
                    "toggle": {
                        "icon": "material/brightness-7",
                        "name": "Switch to dark mode",
                    },
                },
                {
                    "media": "(prefers-color-scheme: dark)",
                    "scheme": "slate",
                    "primary": "custom",
                    "accent": "custom",
                    "toggle": {
                        "icon": "material/brightness-4",
                        "name": "Switch to light mode",
                    },
                },
            ],
        },
        "nav": [
            {"Home": "index.md"},
            {"Survey Documentation": "survey_documentation.md"},
        ],
        "markdown_extensions": [
            "tables",
            {"toc": {"permalink": True, "toc_depth": 3}},
            "admonition",
            "pymdownx.details",
            "pymdownx.superfences",
            {"pymdownx.tabbed": {"alternate_style": True}},
            {"pymdownx.highlight": {"anchor_linenums": True}},
            "pymdownx.inlinehilite",
            "pymdownx.snippets",
            "attr_list",
            "md_in_html",
        ],
        "plugins": [{"search": {"lang": "en", "separator": r"[\s\-\.]"}}],
        "docs_dir": str(docs_path),
        "site_dir": str(site_dir),
        "extra_css": ["stylesheets/extra.css"],
    }


css_content = """/*-- Main Color --*/

:root {
  --md-primary-fg-color: #991766;
  --md-accent-fg-color: #991766;
}

/*-- Logo Size --*/

.md-header__button.md-logo img,
.md-header__button.md-logo svg {
  height: 3rem;
  width: auto;
}

/*-- Table Borders --*/

table {
  border-collapse: collapse;
  width: 100%;
}

table th,
table td {
  border: 1px solid var(--md-default-fg-color--lightest);
  padding: 0.6rem 1rem;
}

table thead th {
  border-bottom: 2px solid var(--md-default-fg-color--light);
  background-color: var(--md-code-bg-color);
}

/* Dark mode adjustments */
[data-md-color-scheme="slate"] table th,
[data-md-color-scheme="slate"] table td {
  border-color: var(--md-default-fg-color--lighter);
}

[data-md-color-scheme="slate"] table thead th {
  border-bottom-color: var(--md-default-fg-color);
}
"""
