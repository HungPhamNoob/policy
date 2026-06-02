import os, sys, logging, glob

# Must be set before h2o init
os.environ['MLFLOW_TRACKING_URI'] = 'http://10.128.0.4:5000'
os.environ['MLFLOW_EXPERIMENT_NAME'] = 'traffic-risk-assessment'

import h2o, mlflow, mlflow.h2o
from h2o.automl import H2OAutoML
from sklearn.metrics import accuracy_score, f1_score

logging.basicConfig(level='INFO', format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('fast-retrain')

logger.info('=== FAST RETRAIN START ===')
h2o.init(max_mem_size='3G', nthreads=2)
mlflow.set_tracking_uri('http://10.128.0.4:5000')
mlflow.set_experiment('traffic-risk-assessment')

# Only use latest Spark batch's CSV files
csv_files = sorted(glob.glob('/opt/traffic/data/cloud/gold/features/retrain/csv/part-*-4211178f-*.csv'))
logger.info(f'Using {len(csv_files)} csv files')
df = h2o.import_file(csv_files)
logger.info(f'Loaded {df.nrows} rows, {df.ncols} cols')
df['true_severity'] = df['true_severity'].asfactor()

excluded = {'event_id', 'event_year', 'event_time', 'true_severity',
            'ingestion_time_epoch', 'processed_time_epoch', 'end_to_end_latency_ms'}
features = [c for c in df.columns if c not in excluded]
logger.info(f'Features={len(features)}')

train, test = df.split_frame(ratios=[0.8], seed=42)
logger.info(f'Train={train.nrows} Test={test.nrows}')

with mlflow.start_run(run_name='h2o_retrain_online') as run:
    aml = H2OAutoML(max_runtime_secs=180, seed=42,
                    balance_classes=True, max_after_balance_size=5.0,
                    sort_metric='mean_per_class_error')
    aml.train(x=features, y='true_severity', training_frame=train)
    logger.info('Training done')

    lb = aml.leaderboard.head(5).as_data_frame()
    logger.info(f'Top5:\n{lb}')

    best = aml.leader
    y_pred = best.predict(test[features]).as_data_frame()['predict'].astype(str)
    y_true = test['true_severity'].as_data_frame()['true_severity'].astype(str)
    acc = accuracy_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred, average='weighted', zero_division=0)
    logger.info(f'Best={best.model_id} acc={acc:.4f} f1={f1:.4f}')

    mlflow.log_metric('accuracy', acc)
    mlflow.log_metric('weighted_f1', f1)
    mlflow.h2o.log_model(best, artifact_path='best_model')
    mv = mlflow.register_model(f'runs:/{run.info.run_id}/best_model', 'traffic-risk-model')
    logger.info(f'REGISTERED model v{mv.version}')

h2o.shutdown(prompt=False)
logger.info('=== FAST RETRAIN DONE ===')