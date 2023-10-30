import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
import joblib
import os
from DownloadCsvFile import download_csv_from_s3  
from CreateEvalMetrics import CreateEvalMetrix

if not os.path.exists('churn.csv'):
    access_key = os.getenv("access_key")
    secret_access_key = os.getenv("secret_access_key")
    region = os.getenv("region")
    s3_bucket = 'luffydatalake'
    s3_key = 'temp/churn.csv'
    
    download_csv_from_s3(access_key, secret_access_key, region, s3_bucket, s3_key, 'churn.csv')
    

label_encoder = LabelEncoder()
scaler = MinMaxScaler()

df = pd.read_csv("churn.csv")

df['balance_to_salary_ratio'] = np.where(df['balance'] == 0, 0, df['balance'] / df['estimated_salary'])
df["Credit_Utilization_Ratio"] = np.where(df['balance'] == 0, 0, df['balance'] / df['credit_score'])

score_ranges = [0, 600, 650, 700, 750, 850]
score_labels = ['Poor', 'Fair', 'Good', 'Very Good', 'Excellent']

df['credit_score_range'] = pd.cut(df['credit_score'], bins=score_ranges, labels=score_labels)

df['country'] = label_encoder.fit_transform(df['country'])
df['gender'] = label_encoder.fit_transform(df['gender'])
df['credit_score_range'] = label_encoder.fit_transform(df['credit_score_range'])

numerical_columns = df.select_dtypes(include=['number']).columns
df[numerical_columns] = scaler.fit_transform(df[numerical_columns])

y = df["churn"]
x = df.drop('churn', axis=1) 

X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.33, random_state=42)

gbm = GradientBoostingClassifier(
    ccp_alpha=0.0,
    criterion='friedman_mse',
    init=None,
    learning_rate=0.1,
    loss='deviance',
    max_depth=3,
    max_features=None,
    max_leaf_nodes=None,
    min_impurity_decrease=0.0,
    min_samples_leaf=1,
    min_samples_split=2,
    min_weight_fraction_leaf=0.0,
    n_estimators=100,
    n_iter_no_change=None,
    random_state=694,
    subsample=1.0,
    tol=0.0001,
    validation_fraction=0.1,
    verbose=0,
    warm_start=False
)

gbm.fit(X_train, y_train)
model_file = 'gradient_boosting_model.pkl'
joblib.dump(gbm, model_file)
y_pred = gbm.predict(X_test)
y_pred = gbm.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
y_pred_prob = gbm.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred_prob)

CreateEvalMetrix(accuracy, f1, precision, recall, auc)
