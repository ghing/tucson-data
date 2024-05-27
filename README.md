# Tucson Data

Tucson data sources, code to fetch the data and to make it more usable. 

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Data sources

### Bicycle Routes

Link: https://gisopendata.pima.gov/datasets/e590f125f2744ad0918c9025363004eb_2/explore 

### Bikeways

Link: https://maps.pagregion.com/PAGBikePed/BikewaysMap

This data has the same schema as the Bicycle Routes data from Pima County, but has fewer features. It doesn't appear to have mountain bike trails, and the `BIKERTE_DS` field seems a bit dirtier than the one in the Bicycle Routes data.

### Jurisdiction Boundaries

Link: https://gisopendata.pima.gov/datasets/472d08edd7324a5bbda029a6d966095d_11

### Libraries

Link: https://gisopendata.pima.gov/datasets/adab4088035540e6bf9babfccd0ee4e6_2/

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `tucson_data/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `tucson_data_tests` directory and you can run tests using `pytest`:

```bash
pytest tucson_data_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
