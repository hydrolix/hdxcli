# Hdx-cli purpose

hdx-cli is a command-line tool to work with hydrolix projects and tables
interactively.

Common operations such as CRUD operations on projects/tables/transforms and others 
can be performed.

# Hdx-cli installation

You can install `hdx-cli` from pip:

```shell
pip install hdx-cli
```

# Using hdx-cli

hdx-cli supports multiple profiles. You can use a default profile or
use the `--profile` option to operate on a non-default profile.

When trying to invoke a command, if a login to the server is necessary, 
a prompt will be shown and the token will be cached.


## Working with projects, tables and transforms

The basic operations you can do with these resources are:

- list them
- create a new resource
- delete an existing resource
- modify an existing resource
- show a resource in raw json format
- show settings from a resource


## Common operations

### Showing help 

In order to see what you can do with the tool:

``` shell
hdx-cli --help
```

### Listing resources

To list projects:

``` shell
hdx-cli project list
```

To list resources on a project:

``` shell
hdx-cli --project-name <project-name> table list
```


You can avoid repeating the project and table name by using the `set` command:


### Set/unset project and table

``` shell
hdx-cli set <your-project> <your-table>
```

Subsequent operations will be applied to the project and table. If you want to `unset`
it, just do:

``` shell
hdx-cli unset
```


### Creating resources

``` shell
hdx-cli project create <project-name>
```


### Peforming operations against another server

If you want to use `hdx-cli` against another server, use `--profile` option:


### Showing resource settings

Creating a project:

``` shell
hdx-cli project create <myprojectname>
```


### Getting help for subcommands

Check which commands are available for each resource by typing:

```
hdx-cli <resource> --help
```





