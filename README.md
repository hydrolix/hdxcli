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

# Using hdxcli command-line program

`hdxcli` supports multiple profiles. You can use a default profile or
use the `--profile` option to operate on a non-default profile.

When trying to invoke a command, if a login to the server is necessary, 
a prompt will be shown and the token will be cached.


# Command-line tool organization

The tool is organized, mostly with the general invocation form of:

```shell
hdxcli <resource> [<subresource...] <verb> [<resource_name>]
```

Table and project resources have defaults that depend on the profile you are working with,
so they can be omitted if you used the `set` command.

For all other resources, you can use `--transform`, `--dictionary`, etc. Please see the
command line help for more information.

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

## Working with transforms and batch jobs

In order to use a transforms, you need to:

1. create a transform


``` shell
hdxcli transform create -f atransform.json atransform
```

Where atransform.json is a local file and atransform is the 
name for the transform that will be uploaded to the cluster. 
Remember that a transform is applied to a table in a project, 
so whatever you `set` with the 
command-line tool will be the target of your transform.

If you want to override it, do:

``` shell
hdxcli --project <the-project> --table <the-table> transform create -f atransform.json atransform
```


2. ingest a batch job

``` shell
hdxcli job batch ingest <job-name> <job-url>
```

The job-name is the job name you will see if you list the job batch. job url can be either a local url or a url
to a bucket *for which the cluster has at lease read access to*.


## Listing and showing your profiles 

Listing profiles:


``` shell
hdxcli profile list
```

Shogin default profile

``` shell
hdxcli profile show
```



## FAQ: Common operations

### Showing help 

In order to see what you can do with the tool:

``` shell
hdxcli --help
```

### Listing resources

To list projects:

``` shell
hdxcli project list
```

To list resources on a project:

``` shell
hdxcli --project <project-name> table list
```


You can avoid repeating the project and table name by using the `set` command:


### Set/unset project and table

``` shell
hdxcli set <your-project> <your-table>
```

Subsequent operations will be applied to the project and table. If you want to `unset`
it, just do:

``` shell
hdxcli unset
```


### Creating resources

``` shell
hdxcli project create <project-name>
```


### Peforming operations against another server

If you want to use `hdxcli` against another server, use `--profile` option:


### Working with resource settings

Show settings for a resource:

``` shell
hdxcli project <myprojectname> settings
```

``` shell
hdxcli table <mytablename> settings
```

``` shell
hdxcli --transform <mytransform transform settings
```


Modify a setting:

``` shell
hdxcli table <mytablename> settings key value
```

Show a single setting:

``` shell
hdxcli table <mytablename> settings key value
```



### Getting help for subcommands

Check which commands are available for each resource by typing:


```
hdxcli [<resource>...] [<verb>] --help
```
