import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression, Lasso, Ridge, ElasticNet
import sklearn
sklearn.set_config(transform_output="pandas")
from sklearn.utils import compute_sample_weight
#
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import confusion_matrix
from sklearn.linear_model import Ridge
from sklearn import metrics
from sklearn.metrics import accuracy_score
from sklearn.base import BaseEstimator, TransformerMixin
import seaborn as sns


from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import plot_tree

from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification

from sklearn.neighbors import KNeighborsClassifier

from sklearn.preprocessing import RobustScaler

from sklearn.cluster import KMeans

from sklearn.ensemble import GradientBoostingClassifier

from sklearn.svm import SVC

import xgboost as xgb
from xgboost import XGBRegressor, XGBClassifier

from sklearn.pipeline import Pipeline

from sklearn.compose import ColumnTransformer

import optuna

import mlflow

import warnings

warnings.simplefilter('ignore')

import joblib

from joblib import dump, load

import requests

#client = mlflow.MlflowClient(tracking_uri = "/app/mlruns")

#mlflow.set_tracking_uri("/app/mlruns")

client = mlflow.MlflowClient(tracking_uri = "http://34.121.20.1:5000")
mlflow.set_tracking_uri("http://34.121.20.1:5000")


import requests

from sklearn import impute

from sklearn.impute import KNNImputer

from sklearn.experimental import enable_iterative_imputer

from sklearn.impute import IterativeImputer

from imblearn.over_sampling import SMOTE

from imblearn.pipeline import Pipeline

from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_curve, auc, roc_auc_score
from sklearn.metrics import precision_recall_curve, average_precision_score
RN_STATE = 42



CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

def run():
    #spark = SparkSession.builder.appName("ExampleJob111").getOrCreate()
    spark = (
            SparkSession.builder
            .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
            .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
            .config("spark.sql.catalog.clickhouse.host", CH_IP)
            .config("spark.sql.catalog.clickhouse.protocol", "http")
            .config("spark.sql.catalog.clickhouse.http_port", "8123")
            .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
            .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD) 
            .config("spark.sql.catalog.clickhouse.database", "default")
            .config("spark.clickhouse.write.format", "json")
            .getOrCreate()
        )
    sc = spark.sparkContext
    spark.sql("use clickhouse")
    spark.sql("Select * from bank.salaryprediction_2").show()


    print("Чтение данных из ClickHouse...")
    df = spark.sql("SELECT * FROM bank.salary_prediction2").toPandas()

    numerical = Pipeline(steps=[
    ("SimpleImputer", SimpleImputer()),
    #("Imputer", KNNImputer()),
    ('Scaler', StandardScaler())
])

categorical = Pipeline(steps=[
    ("SimpleImputer", SimpleImputer(strategy='constant')),
    ("OneHotEncoder", OneHotEncoder(handle_unknown='ignore', #drop=['first', 'if_binary'],
                                    sparse_output=False))
])

ct = ColumnTransformer([
    ("numerical", numerical, numerical_columns),
    ("categorical", categorical, categorical_columns)
], remainder = 'passthrough')


pipe = Pipeline(steps=[
    ("ct", ct),
    ("SMOTE", SMOTE(random_state=RN_STATE)),
    ("Imputer", KNNImputer()),
    ("XGBClassifier", XGBClassifier(random_state=RN_STATE))
])


counter = 0

counter += 1
def gridsearch_replacment(trial):

    params = {
        'XGBClassifier__n_estimators': trial.suggest_int('XGBClassifier__n_estimators', 1, 100) ,
        'XGBClassifier__learning_rate': trial.suggest_float('XGBClassifier__learning_rate', 0.01, 0.1),
        'XGBClassifier__max_depth': trial.suggest_int('XGBClassifier__max_depth', 5, 15),
        'XGBClassifier__gamma': trial.suggest_int('XGBClassifier__gamma', 1, 6),

    }

    pipe.set_params(**params)
    score = cross_val_score(pipe, X_train, y_train, cv=5, scoring = 'f1_macro').mean()
    return score

study = optuna.create_study(direction='maximize')
study.optimize(gridsearch_replacment, n_trials=10)

study.best_params

study.best_value

pipe.set_params(**study.best_params)
pipe.fit(X_train, y_train)

# Проверьте, существует ли эксперимент
experiment_name = "XGBClassifier12/10/24"
experiment = mlflow.get_experiment_by_name(experiment_name)

if experiment is None:
    # Создайте эксперимент, если он не существует
    experiment_id = mlflow.create_experiment(experiment_name)
else:
    # Используйте существующий эксперимент
    experiment_id = experiment.experiment_id

artifact_path = "models/best_model"

with mlflow.start_run(experiment_id=experiment_id, run_name=f'{counter}_experiment'):
    st_b = study.best_params
    mlflow.log_params(st_b)
    stud_bv = study.best_value

    pipe.set_params(**st_b)
    pipe.fit(X_train, y_train)
    #mlflow.sklearn.log_model(pipe, 'best_model')
    mlflow.sklearn.log_model(pipe, artifact_path=artifact_path, registered_model_name='best_model')

    pred_test = pipe.predict(X_test)
    f1_test = f1_score(pred_test, y_test, average='macro')

    log_sum = {'f1_macro_cv5': stud_bv, 'f1_score_macro': f1_test}
    mlflow.log_metrics(log_sum)


stud_bv
st_b

erorr_xtest= f1_score(pred_test, y_test)
erorr_xtest


import mlflow
logged_model = 'runs:/fea0c19d7e8c4482bba03549fc74a768/models/best_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd
#loaded_model.predict(pd.DataFrame(X_test))