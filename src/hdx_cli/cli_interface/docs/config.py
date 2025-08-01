from typing import List, Dict, Any

# This structure defines the logical grouping for a full documentation export.
DOC_STRUCTURE: List[Dict[str, Any]] = [
    {
        "filename": "cli-configuration.md",
        "title": "CLI Configuration",
        "description": "Commands to configure the CLI. Manage connection profiles to your clusters and set default contexts (project/table) to streamline your operations.",
        "commands": ["init", "profile", "set", "unset"],
    },
    {
        "filename": "resource-management.md",
        "title": "Resource Management",
        "description": "Commands for the lifecycle (create, list, delete, modify) of core Hydrolix resources. Use these to manage the structure of your data.",
        "commands": ["project", "table", "dictionary", "function", "transform", "view", "column"],
    },
    {
        "filename": "data-and-jobs.md",
        "title": "Data & Jobs",
        "description": "Commands for data ingestion and process management. This covers real-time streaming, batch jobs, and storage configuration.",
        "commands": ["stream", "job", "source", "storage"],
    },
    {
        "filename": "security-and-access.md",
        "title": "Security & Access Control",
        "description": "Commands for security management. Administer user and service accounts, assign roles and permissions, and configure credentials for external systems.",
        "commands": ["user", "service-account", "role", "credential"],
    },
    {
        "filename": "utilities-and-operations.md",
        "title": "Utilities & High-Level Operations",
        "description": "Commands for advanced and utility operations. Find tools for cross-cluster migrations, integrity checks, resource pool management, and resource summaries here.",
        "commands": ["migrate", "check-health", "shadow", "pool", "integration", "query-option", "resource-summary"],
    },
]