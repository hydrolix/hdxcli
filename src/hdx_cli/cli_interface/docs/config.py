from typing import Any, Dict, List

# This structure defines the logical grouping for a full documentation export.
DOC_STRUCTURE: List[Dict[str, Any]] = [
    {
        "filename": "hdxcli-configuration.md",
        "commands": ["init", "profile", "set", "unset"],
        "frontmatter": {
            "title": "Configuration",
            "slug": "hdxcli-configuration",
            "excerpt": "Commands to configure the HDXCLI. Manage connection profiles to your clusters and set default contexts (project/table) to streamline your operations.",
            "category": "65774de90f4b3f001039021c",
            "hidden": False,
        },
    },
    {
        "filename": "hdxcli-resource-management.md",
        "commands": ["project", "table", "dictionary", "function", "transform", "view", "column"],
        "frontmatter": {
            "title": "Resource Management",
            "slug": "hdxcli-resource-management",
            "excerpt": "Commands for the lifecycle (create, list, delete, modify) of core Hydrolix resources. Use these to manage the structure of your data.",
            "category": "65774de90f4b3f001039021c",
            "hidden": False,
        },
    },
    {
        "filename": "hdxcli-data-and-jobs.md",
        "commands": ["stream", "job", "source", "storage"],
        "frontmatter": {
            "title": "Data & Jobs",
            "slug": "hdxcli-data-and-jobs",
            "excerpt": "Commands for data ingestion and process management. This covers real-time streaming, batch jobs, and storage configuration.",
            "category": "65774de90f4b3f001039021c",
            "hidden": False,
        },
    },
    {
        "filename": "hdxcli-security-and-access.md",
        "commands": ["user", "service-account", "role", "row-policy", "credential"],
        "frontmatter": {
            "title": "Security & Access Control",
            "slug": "hdxcli-security-and-access",
            "excerpt": "Commands for security management. Administer user and service accounts, assign roles and permissions, define row-level access policies and configure credentials for external systems.",
            "category": "65774de90f4b3f001039021c",
            "hidden": False,
        },
    },
    {
        "filename": "hdxcli-utilities-and-operations.md",
        "commands": [
            "migrate",
            "check-health",
            "shadow",
            "pool",
            "integration",
            "query-option",
            "show-defaults",
            "resource-summary",
        ],
        "frontmatter": {
            "title": "Utilities & High-Level Operations",
            "slug": "hdxcli-utilities-and-operations",
            "excerpt": "Commands for advanced and utility operations. Find tools for cross-cluster migrations, integrity checks, resource pool management, and resource summaries here.",
            "category": "65774de90f4b3f001039021c",
            "hidden": False,
        },
    },
]
