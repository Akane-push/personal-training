import polars as pl
import polars.selectors as cs

from joblib import dump

from pathlib import Path
import sys
import os
from data_cleaning import DataCleaning as cl

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
#from sklearn.metrics import classification_report
from xgboost import XGBClassifier

filename_flight = "_flightdatas.parquet"
filename_weather = "_weatherdatas.parquet"

datas_path = "/opt/airflow/output"
save_dir = "/opt/airflow/output"

class XGBGridSearch():
    def __init__(self):
        df_flights = pl.read_parquet(f"{datas_path}/*{filename_flight}")
        df_weather = pl.read_parquet(f"{datas_path}/*{filename_weather}")
    
        df_clean = cl(df_flights, df_weather).get_dataframe()

        feats = df_clean.drop("Delay")
        target = df_clean["Delay"]
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(feats, target, test_size = 0.2, random_state = 42)

        self.oe = OrdinalEncoder(handle_unknown = 'use_encoded_value', unknown_value = -1)

        self.x_train = self.encoding(self.x_train)
        self.x_test = self.encoding(self.x_test)


    #Converts columns to match the model
    def encoding(self, df):
        numerical_cols = df.select(cs.numeric()).columns
        categorical_cols = df.select(cs.string()).columns

        train_numerical = df[numerical_cols]
        train_categorical = df[categorical_cols]

        imputer_median = SimpleImputer(strategy='median')
        imputer_most_frequent = SimpleImputer(strategy='most_frequent')

        train_numerical[train_numerical.columns] = imputer_median.fit_transform(train_numerical)
        train_categorical[train_categorical.columns] = imputer_most_frequent.fit_transform(train_categorical)

        train_categorical = train_categorical[sorted(train_categorical.columns)]
        train_categorical[train_categorical.columns] = self.oe.fit_transform(train_categorical)

        df = pl.concat([train_numerical, train_categorical],  how="horizontal")
        df = df[sorted(df.columns)]
        return df

    #Testing parameters using GridSearch
    def found_parameters(self):
        model = XGBClassifier(objective='binary:logistic', eval_metric='auc')

        param_grid = {'max_depth': [3, 4, 6],
                        'learning_rate': [0.3, 0.1, 0.05],
                        'n_estimators': [100, 200],
                        'gamma': [0, 0.05, 0.1, 0.15]#,
                        #'subsample': [0.8, 0.85, 0.9],
                        #'colsample_bytree': [0.8, 0.85, 0.9]
                    }
        
        grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=3, n_jobs=-1, verbose=2)
        grid_search.fit(self.x_train, self.y_train)

        print("Best parameters found: ", grid_search.best_params_)
        print("Best score: ", grid_search.best_score_)

        return grid_search.best_estimator_


    #Encodes the model with the selected parameters
    def training_model(self):
        params = {'objective': 'binary:logistic',
                    'eval_metric': 'auc',
                    'max_depth': 3,
                    'learning_rate': 0.3,
                    'n_estimators': 100,
                    'gamma': 0,}
        
        bst = XGBClassifier(**params)
        bst.fit(self.x_train, self.y_train)

        dump(bst, os.path.join(save_dir, 'md.joblib'))
        dump(self.oe, os.path.join(save_dir, 'oe.joblib'))

        print(f"Current Directory: {os.getcwd()}")

        print('Model saved as md.joblib')
        print('Encoder saved as oe.joblib')

if __name__ == "__main__":
    XGBGridSearch().found_parameters()