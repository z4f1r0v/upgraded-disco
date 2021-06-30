<h1>Delta Pipelines Example Project</h1>

<p align="center">
  <img src="https://delta.io/wp-content/uploads/2019/04/delta-lake-logo-tm.png" width="140"/><br>
  <strong>Delta Pipelines</strong> is a new framework designed to enable customers to successfully declaratively define, deploy, test & upgrade data pipelines and eliminate operational burdens associated with the management of such pipelines.
</p>
<p align="center">
  This project is an example of a Delta Pipeline, designed to get customers started with building, deploying and running pipelines.
</p>

## Getting Started

#### 1. Clone this repo:

```bash 
git clone git@github.com:databricks/delta-pipelines-example-scala.git
```

#### 2. Build the project:

If the goal is to develop the project further, you'll probably want to add it to your favourite IDE as a new project from existing sources. If you just want to build and run it, you should be ok just running the sbt package command:

```bash
build/sbt package
```

If you've added it to your IDE, just create a new run configuration that invokes the sbt package task

The package command is going to build a jar for your pipeline in the following location:
```bash
target/scala-2.12/delta-pipelines-example-scala_2.12-0.0.1-SNAPSHOT.jar
```
#### 3. Setup the Databricks CLI:
Full documentation can be found [here](https://docs.databricks.com/dev-tools/cli/index.html).

CLI version 0.12.0 is required to deploy a new pipeline, and version 0.14.0 has the latest pipelines commands.

If you are a running an earlier version you should first uninstall it. If you have existing environments, it might be cleaner to use conda to create a new one:
```bash
conda create --name delta-pipelines python=3.8
conda activate delta-pipelines
pip install databricks-cli
```
You might also find you want to upload the pipeline to different Databricks environments. [Databricks CLI profiles](https://docs.databricks.com/dev-tools/cli/index.html#connection-profiles) are the best way to do this:
```bash
databricks configure --profile <profile-name>
```
#### 4. Deploy the Pipeline:
To do this you're going to use the new ``pipelines`` CLI command, passing it a Pipeline Spec.

A pipeline spec is used to specify a collection of code and configuration that is needed to run a full pipeline in a given environment (e.g., dev / staging / prod). A spec is a JSON document and it is a common practice to check the spec in alongside the code that is being run. 

In our case the pipeline spec is ``wiki.json`` and can be found in the root of this repo. As you will see if you open it up, the pipeline spec is going to contain a number of things, including the filepath to the jar we built above, and the classpath to our example pipeline. It could additionally contain cluster specs, other code dependencies (libraries) etc...

To create a new pipeline, you can simply run the following. 
```bash
cd delta-pipelines-example-scala/
databricks pipelines deploy wiki.json --profile <profile-name>
```

You should get a response saying that the pipeline has been successfully deployed, the ID of the newly created pipeline, and a URL you can use to take you to the pipeline:
```bash
30cbebf7-9008-4b9a-87ae-4ca5bb95eca0
Pipeline successfully deployed: https://...
```

You should create a new "id" field in your pipeline spec, and insert the logged ID there. Then, the next time you call `databricks pipelines deploy` with the same spec, it will update the pipeline you just created (instead of creating a new one). Alternatively, you can also explicitly specify an ID using the `--pipeline_id` parameter as follows:
```bash
cd delta-pipelines-example-scala/
databricks pipelines deploy wiki.json --profile <profile-name> --pipeline_id 30cbebf7-9008-4b9a-87ae-4ca5bb95eca0
```
If you've lost the ID of a pipeline you've created, you can use the `databricks pipelines list` command to see all the pipelines you own.
