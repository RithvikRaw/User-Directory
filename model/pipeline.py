from model.tasks import (
    initialise_pipeline,
    get_training_data, 
    get_prediction_data,
    import_training_data,
    import_prediction_data,
    process_data, 
    validate_schema, 
    train_model,
    evaluate_model,
    save_model,
    predict,
    load_model,
    write_to_log
)

function_sequence = [
    'import_training_data',
    'get_training_data',
    'process_data',
    'validate_schema',
    'train_model',
    'evaluate_model',
    'save_model',
    'import_prediction_data',
    'get_prediction_data',
    'predict'
]

def training_pipeline():
    context = {}

    pipeline = [
        initialise_pipeline,
        get_training_data,
        get_prediction_data,
        process_data, 
        validate_schema,
        train_model,
        evaluate_model,
        save_model,
        write_to_log
    ]
    
    for task in pipeline:
        task(context)
        
def prediction_pipeline():
    context = {}
    
    pipeline = [
        import_training_data,
        get_prediction_data,
        process_data,
        validate_schema,
        load_model,
        predict
    ]
    
    for task in pipeline:
        task(context)
        
    return context["predicted_data"], context["all_users"]
