# !WARNING! this tap is work in progress and lack tests.

# tap-yfinance

`tap-yfinance` is a Singer tap for [yfinance](https://pypi.org/project/yfinance/).

Built with the [Meltano SDK for Singer Taps](https://gitlab.com/meltano/singer-sdk).

## Installation

```bash
pip install tap-yfinance
```

## Configuration

### Accepted Config Options

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-yfinance --about 
```

## Usage

You can easily run `tap-yfinance` by itself.

### Executing the Tap Directly

```bash
tap-yfinance --version
tap-yfinance --help
tap-yfinance --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pip install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_yfinance/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-yfinance` CLI interface directly using `poetry run`:

```bash
cp config.json{.dist,}
poetry run tap-yfinance --config config.json
```

### Install updated tap

```bash
meltano install extractor tap-yfinance
```

### SDK Dev Guide

See the [dev guide](https://gitlab.com/meltano/singer-sdk/-/blob/main/docs/dev_guide.md) for more instructions on how to use the SDK to 
develop your own taps and targets.

### Generate new schema

TODO: describe native way of creating and updating schemas

[https://www.liquid-technologies.com/online-json-to-schema-converter](https://www.liquid-technologies.com/online-json-to-schema-converter)

WARNING: This tool relies on data in provided JSON to generate data types. If the sample json dataset does not cover all use cases it may result in json validation errors for valid json responses. Always manually verify schemas after using this tool.
