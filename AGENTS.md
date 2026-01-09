# MilleGrilles midcompte project

This is a python project that supports multiple applications for the MilleGrilles system.

## Development environment
+ Basic technology stack for python 3.13.

## Project description

This library contains several independant applications.

**Layout**

+ millegrilles_ceduleur: Moduler for the timing module that emits *pings* every minute on the bus.
+ millegrilles_certissuer: Certificat signing application.
+ millegrilles_media: Media extraction and conversion application.
+ millegrilles_midcompte: Middleware account handler for databases (MongoDB) and RabbitMQ.
+ millegrilles_relaiweb: Web command relay using the requests module.
+ millegrilles_solr: Solr full-text index relay connected to the MilleGrilles bus.
+ millegrilles_streaming: Media streaming application.

## Tool usage
+ When using tool **edit_file** to update an existing file, the tool will likely fail to apply the changes. When the tool fails to apply changes on the first try, switch to the **overwrite** operation.

> **Important:** All future modifications to the project should be performed with the `overwrite` mode of the `edit_file` tool.  The `edit` mode is unreliable for inserting new lines or making non‑trivial changes, so to avoid accidental omissions, always supply the complete file content when editing.

## Development

- Do not add dependencies unless it is explicitly requested.
