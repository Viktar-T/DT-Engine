import matplotlib.pyplot as plt
import seaborn as sns

class DataVisualizer:
    def __init__(self, df):
        self.df = df

    def plot_column(self, column_name):
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=self.df, x=self.df.index, y=column_name)
        plt.title(f'Time Series Plot of {column_name}')
        plt.xlabel('Index')
        plt.ylabel(column_name)
        plt.show()

    def plot_histogram(self, column_name):
        plt.figure(figsize=(10, 6))
        sns.histplot(self.df[column_name], bins=30, kde=True)
        plt.title(f'Histogram of {column_name}')
        plt.xlabel(column_name)
        plt.ylabel('Frequency')
        plt.show()

    def plot_columns(self, column_names):
        for column_name in column_names:
            self.plot_column(column_name)

    def plot_parameter_vs_parameter(self, x_column, y_column):
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=self.df, x=x_column, y=y_column)
        plt.title(f'{y_column} vs {x_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.show()

    def plot_parameter_vs_parameters(self, x_column, y_columns):
        for y_column in y_columns:
            plt.figure(figsize=(10, 6))
            sns.lineplot(data=self.df, x=x_column, y=y_column)
            plt.title(f'{y_column} vs {x_column}')
            plt.xlabel(x_column)
            plt.ylabel(y_column)
            plt.show()

    # ...additional visualization methods as needed...
