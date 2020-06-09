#!/usr/bin/env python3
import os
import json
import singer
import asyncio
import concurrent.futures
from singer import utils, metadata
from singer.catalog import Catalog

from tap_fastly.sync import FastlyAuthentication, FastlyClient, FastlySync

REQUIRED_CONFIG_KEYS = ["start_date",
                        "api_token"]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def discover():
    raw_schemas = load_schemas() 
    streams = []

    for schema_name, schema in raw_schemas.items():  
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []
        
        stream_schema = schema['streams'][0]['schema'] 
        
        stream_metadata = schema['streams'][0]['metadata'] 
        stream_key_properties = schema['streams'][0]['metadata'][0]['metadata']['table-key-properties'] #populates key properties




        # create and add catalog entry
        catalog_entry = { 
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': stream_schema,
            'metadata' : stream_metadata, 
            'key_properties': stream_key_properties
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']: 
        stream_metadata = metadata.to_map(stream['metadata'])
        
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def create_sync_tasks(config, state, catalog):
    auth = FastlyAuthentication(config["api_token"])
    client = FastlyClient(auth)
    sync = FastlySync(client, state, config)

    selected_stream_ids = get_selected_streams(catalog)

    sync_tasks = (sync.sync(stream['tap_stream_id'], stream['schema'])
                  for stream in catalog['streams']
                  if stream['tap_stream_id'] in selected_stream_ids)

    return asyncio.gather(*sync_tasks)

def sync(config, state, catalog):
    loop = asyncio.get_event_loop()
    try:
        tasks = create_sync_tasks(config, state, catalog)
        loop.run_until_complete(tasks)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        config = args.config
        state = {
            "bookmarks": {
                "bills": {"start_time": config["start_date"]}
            }
        }
        state.update(args.state)

        sync(config, state, catalog)

if __name__ == "__main__":
    main()
