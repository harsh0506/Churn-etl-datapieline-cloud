import csv

def CreateEvalMetrix(accuracy, f1, precision, recall, auc):
    # Create a dictionary to store the metrics
    metrics_dict = {
        'Metric': ['Accuracy', 'F1-Score', 'Precision', 'Recall', 'AUC'],
        'Value': [accuracy, f1, precision, recall, auc]
    }

    # Define the CSV file name
    csv_file_name = 'metrics.csv'

    # Write the dictionary to a CSV file
    with open(csv_file_name, 'w', newline='') as csvfile:
        fieldnames = ['Metric', 'Value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write the header
        writer.writeheader()
        
        # Write the metrics
        for metric, value in zip(metrics_dict['Metric'], metrics_dict['Value']):
            writer.writerow({'Metric': metric, 'Value': value})

    print(f'Metrics saved to {csv_file_name}')
