[![](images/hdxcli.png)](https://github.com/hydrolix/hdx-cli)


`hdxcli` is a command-line tool to work with hydrolix projects and tables
interactively.

Common operations such as CRUD operations on projects/tables/transforms 
and others  can be performed.

# Hdx-cli installation

You can install `hdxcli` from pip:

```shell
pip install hdxcli
```
## System Requirements
Python version `>= 3.10` is required.

Make sure you have the correct Python version installed before proceeding 
with the installation of `hdxcli`.

# Usage

## Command-line tool organization

The tool is organized, mostly with the general invocation form of:

```shell
hdxcli <resource> [<subresource...] <verb> [<resource_name>]
```

Table and project resources have defaults that depend on the profile
you are working with, so they can be omitted if you previously used 
the `set` command.

For all other resources, you can use `--transform`, `--dictionary`, 
`--source`, etc. Please see the command line help for more information.

## Profiles
`hdxcli` supports multiple profiles. You can use a default profile or
use the `--profile` option to operate on a non-default profile.

When trying to invoke a command, if a login to the server is necessary, 
a prompt will be shown and the token will be cached.

## Listing and showing profiles

Listing profiles:
```shell
hdxcli profile list
```

Showing default profile:
```shell
hdxcli profile show
```

## Projects, tables and transforms

The basic operations you can do with these resources are:

- list them
- create a new resource
- delete an existing resource
- modify an existing resource
- show a resource in raw json format
- show settings from a resource
- write a setting
- show a single setting

## Working with transforms

You can create and override transforms with the following commands.

Create a transform:
``` shell
hdxcli transform create -f <transform-settings-file> <transform-name>
```

Remember that a transform is applied to a table in a project, so whatever 
you set with the command-line tool will be the target of your transform.


If you want to override it, do:

``` shell
hdxcli --project <project-name> --table <table-name> transform create -f <transform-settings-file>.json <transform-name>
```

## Ingest
### Batch Job
Create a batch job:

``` shell
hdxcli job batch ingest <job-name> <job-settings>.json
```

`job-name` is the name of the job that will be displayed when listing batch 
jobs. `job-settings` is the path to the file containing the specifications 
required to create that ingestion (for more information on the required 
specifications, see Hydrolix API Reference).

In this case, the project, table, and transform are being omitted and the 
CLI will use the default transform within the project and table previously 
configured in the profile with the `--set` command. Otherwise, you can add 
`--project <project-name>, --table <table-name> --transform <transform-name>`.

This allows you to execute the command as follows:
``` shell
hdxcli --project <project-name>, --table <table-name> --transform <transform-name> job batch ingest <job-name> <job-settings>.json
```

# Commands

- Profile
  - *list*
    - `hdxcli profile list`
  - *add*
    - `hdxcli profile add <profile-name>`
  - *show*
    - `hdxcli --profile <profile-name> profile show`
- Set/Unset
  - *set*
    - `hdxcli set <project-name> <table-name>`
  - *unset*
    - `hdxcli unset`
- Project
  - *list*
    - `hdxcli project list`
  - *create*
    - `hdxcli project create <project-name>`
  - *delete*
    - `hdxcli project delete <project-name>`
  - *activity*
    - `hdxcli --project <project-name> project activity`
  - *stats*
    - `hdxcli --project <project-name> project stats`
  - *show*
    - `hdxcli --project <project-name> project show`
  - *settings*
    - `hdxcli --project <project-name> project settings`
    - `hdxcli --project <project-name> project settings <setting-name>`
    - `hdxcli --project <project-name> project settings <setting-name> <new-value>`
- Table
- Transform
- Job
- Purgejobs
- Sources
- Dictionary
- Dictionary Files
- Function
- Storage
- Integration
- Migrate
- Version

# FAQ: Common operations

## Showing help 

In order to see what you can do with the tool:

``` shell
hdxcli --help
```

Check which commands are available for each resource by typing:
``` shell
hdxcli [<resource>...] [<verb>] --help
```

## Performing operations against another server

If you want to use `hdxcli` against another server, use `--profile` option:
``` shell
hdxcli --profile <profile-name> project list
```

## Obtain indented resource information

When you use the verb `show` on any resource, the output looks like this:
``` shell
hdxcli --project <project-name> project show
{"name": "project-name", "org": "org-uuid", "description": "description", "uuid": "uuid", ...}
```

If you need to have an indented json version, just add `-i`, `--indent int`:
``` shell
hdxcli --project <project-name> project show -i 4
{
    "name": "project-name", 
    "org": "org-uuid", 
    "description": "description", 
    "uuid": "uuid", 
    ...,
}
```