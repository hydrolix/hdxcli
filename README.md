[![](images/hdxcli.png)](https://github.com/hydrolix/hdx-cli)


`hdxcli` is the command-line tool to work with your Hydrolix clusters. It helps you manage resources like projects, tables, and Service Accounts. You can use it to automate tasks and include Hydrolix in your scripts and workflows.


## System Requirements
- Python: `>= 3.10`

Make sure you have the correct Python version installed.

## Installation
You can install hdxcli using pip:
```shell
  pip install hdxcli
```

# First Steps: Initial Setup
When you run your first `hdxcli` command (for example, `hdxcli project list`), if the CLI does not find a previous setup, it will guide you to create a 'default' connection profile. You will need to enter:

1. The **hostname** of your Hydrolix cluster (e.g., `mycluster.hydrolix.live`).
2. If the connection will use **TLS (https)** (recommended).

After setting up the profile, you will be asked to log in with your Hydrolix **username and password**. After a successful login, you can choose how the CLI will authenticate for future operations:

- Continue using your user credentials.
- Set up the CLI to use a **Service Account**. This is useful for longer sessions or for automated scripts.

You can also start this setup process yourself by running:
```shell
  hdxcli init
```
This command is good if you prefer to set up the CLI before running other commands.

## General Usage
The main way to use commands is:

`hdxcli [GLOBAL OPTIONS] RESOURCE [ACTION] [SPECIFIC ARGUMENTS...]`

For example, `hdxcli project list` or `hdxcli table --project myproject create mytable`

### Common Global Options (see hdxcli --help for all options):
- `--profile PROFILE_NAME`: Use a specific connection profile.
- `--username USERNAME`: Username for login (if needed).
- `--password PASSWORD`: Password for login (if `--username` is used).
- `--uri-scheme [http|https]</var>`: Choose the connection scheme (http or https).
- `--timeout SECONDS`: Timeout for API requests.

- ### Connection Profiles
Profiles let you save settings for different Hydrolix clusters or users.

- List profiles: `hdxcli profile list`
- View details of the 'default' profile: `hdxcli profile show default`
- Use a profile in a command: `hdxcli --profile my_other_profile project list`

### Default Project and Table Context
To make commands simpler, you can set a "current" or "default" project and table.

- Set default project and table:
    ```shell
    hdxcli set <project-name> <table-name>
    ```
    Example: `hdxcli set weblogs access_logs`

- After setting defaults, commands for tables or transforms will not need `--project` or `--table` options:
    ```shell
    hdxcli transform show my_transform # Will use project and table set by 'set' command
    ```

- Clear default project and table:
    ```shell
    hdxcli unset
    ```
  
## Main Commands (Summary)
`hdxcli` commands are grouped by the type of resource they manage. Use `hdxcli --help` to see all commands. Some of the main groups are:

- `profile`: Manage your connection profiles.
- `init`: Initialize `hdxcli` configuration.
- `set` / `unset`: Set or clear the default project/table.
- `project`: Create, list, delete, and manage projects.
- `table`: Manage tables inside projects.
- `transform`: Manage transforms.
- `service-account`: Manage Service Accounts and their tokens.
- `job`: Manage ingestion jobs.
- (Other important groups like `dictionary`, `function`, `storage`, etc.)
- `version`: Show the `hdxcli` version.

To get help for a specific command group or command:
```shell
    hdxcli project --help
    hdxcli project create --help
```

### Usage Examples
1. Set up the CLI, log in, and list projects:
    ```shell
    $ hdxcli init
    # ... follow prompts to set up hostname, scheme, and login ...
    # ... optionally, set up a Service Account ...
    
    $ hdxcli project list
    project_a
    project_b
    ```

2. Create a new project and then a table:
    ```shell
    $ hdxcli project create my_new_project
    Created project 'my_new_project'
    
    $ hdxcli table --project my_new_project create my_new_table
    Created table 'my_new_table'
    ```

3. Set a default context and show transform details:
    ```shell
    $ hdxcli set my_new_project my_new_table
    Profile 'default' set project/table
    
    $ hdxcli transform show my_existing_transform
    # ... (output of the transform) ...
    ```

4. Show project information in indented JSON format:
    ```shell
    $ hdxcli project show my_new_project -i
    {
        "name": "my_new_project",
        "org_id": "xxxx-xxxx-xxxx-xxxx",
        ...
    }
    ```
### Getting Help
- For an overview of commands: `hdxcli --help`
- For help on a specific resource or action: `hdxcli <resource> --help` or `hdxcli <resource> <action> --help`
- For more in-depth information, check out the [official Hydrolix documentation](https://docs.hydrolix.io/docs/hdxcli).

## License

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

This project is licensed under the terms of the **Apache License 2.0**.
You can find a copy of the license in the [LICENSE](LICENSE) file included in this repository.