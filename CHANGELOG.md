# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed

## [0.12.0]
### Added
- Add the ability to use bulk indexing when committing logs to Elasticsearch
### Changed
- Only create Elasticsearch client once, instead of recreating it before every commit

## [0.11.8]
### Changed
- Retry connecting to kafka during startup if kafka cluster is not available (configurable)

## [0.11.7]
### Changed
- Enable toggle-able verbose mode

## [0.11.6]
### Changed
- Set round robin strategy as default for consumer rebalancing, also allow it to be configured

## [0.11.5]
### Changed
- Recreate cluster admin whenever barito-flow needs to create a new topic

## [0.11.4]
### Changed
- Update rate limiter behaviour for produce batch, now it counts rate per line in batch instead of per batch request

## [0.11.3]
### Changed
- Lower metadata refresh frequency

## [0.11.2]
### Fixed
- Refresh metadata before creating new topic (see [here](https://github.com/Shopify/sarama/issues/1162))

## [0.11.1]
### Fixed
- Rate limiter on produce batch should count per request, not per log

## [0.11]
### Added
- Ability to process batch logs

## [0.10.1]
### Fixed
- Use updated instru that avoid race condition problem on counter

## [0.10.0]
### Added
- Implement Elasticsearch backoff functionality

## [0.9.0]
### Changed
- Upgrade sarama to 1.19.0
- Use sarama ClusterAdmin for creating topic

## [0.8.5]
### Changed
- Produce logs if flow has to return 5xx errors

## [0.8.4]
### Changed
- Use updated instru that avoid race condition problem

## [0.8.3]
### Changed
- Also send application secret from context when sending metrics

## [0.8.2]
### Changed
- Send metrics to market

## [0.8.1]
### Fixed
- Fix bug when checking for new events

## [0.8.0]
### Added
- Support for multiple topics and indexes

## [0.7.0]
### Added
- Graceful Shutdown

## [0.6.0]
### Changed
- Using consul to get kafka brokers, if consul not available then using environment variable
- Using consul to get elasticsearch url, if url not available then using environment variable

## [0.5.0]
### Changed
- Rate limit trx per second (by default 100)

## [0.4.0]
### Changed
- Rename receiver to producer
- Rename forwarder to consumer
