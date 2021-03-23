import logging
import json
import os

import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    
    # get user input from http request
    json_config = context.get_input()

    result = yield context.call_activity('CreateDiagram', json_config['workspaces'])

    return [result]

main = df.Orchestrator.create(orchestrator_function)