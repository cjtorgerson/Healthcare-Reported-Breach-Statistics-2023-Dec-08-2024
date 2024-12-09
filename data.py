import pandas as pd

def save_to_csv(dataframe, output_path):
    """
    Save a DataFrame to a CSV file.

    :param dataframe: The DataFrame to save.
    :param output_path: Path to save the output CSV file.
    :return: None
    """
    try:
        dataframe.to_csv(output_path, index=False)
        print(f"Data successfully saved to {output_path}")
    except Exception as e:
        print(f"An error occurred while saving to CSV: {e}")

def summarize_covered_entity(file_path):
    """
    Summarize the number of entries for each unique 'Name of Covered Entity' in the CSV file.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the count of entries per covered entity.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Group by 'Name of Covered Entity' and count the number of entries for each
        summary = data['Name of Covered Entity'].value_counts().reset_index()
        summary.columns = ['Name of Covered Entity', 'Count']
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def summarize_breaches_by_month(file_path):
    """
    Summarize the count of breaches by month.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the count of breaches per month.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Ensure 'Breach Submission Date' is a datetime type
        data['Breach Submission Date'] = pd.to_datetime(data['Breach Submission Date'])
        
        # Extract the month and year from the date
        data['Month'] = data['Breach Submission Date'].dt.to_period('M')
        
        # Group by 'Month' and count the number of entries
        summary = data['Month'].value_counts().reset_index()
        summary.columns = ['Month', 'Count']
        
        # Sort by month
        summary = summary.sort_values(by='Month')
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def summarize_breaches_by_year(file_path):
    """
    Summarize the count of breaches by year and calculate the percent increase versus the previous year.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the count of breaches per year and percent increase compared to the previous year.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Ensure 'Breach Submission Date' is a datetime type
        data['Breach Submission Date'] = pd.to_datetime(data['Breach Submission Date'])
        
        # Extract the year from the date
        data['Year'] = data['Breach Submission Date'].dt.year
        
        # Group by 'Year' and count the number of entries
        summary = data['Year'].value_counts().reset_index()
        summary.columns = ['Year', 'Count']
        
        # Sort by year
        summary = summary.sort_values(by='Year')
        
        # Calculate percent increase compared to the previous year
        summary['Percent Increase'] = summary['Count'].pct_change() * 100
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def summarize_individuals_affected_by_breach_type(file_path):
    """
    Summarize the total number of individuals affected grouped by type of breach.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the sum of individuals affected per type of breach.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Ensure 'Individuals Affected' is numeric
        data['Individuals Affected'] = pd.to_numeric(data['Individuals Affected'], errors='coerce')
        
        # Group by 'Type of Breach' and sum the number of individuals affected
        summary = data.groupby('Type of Breach', as_index=False)['Individuals Affected'].sum()
        summary = summary.rename(columns={'Individuals Affected': 'Total Individuals Affected'})
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def summarize_individuals_affected_by_year(file_path):
    """
    Summarize the total number of individuals affected grouped by year.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the sum of individuals affected per year.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Ensure 'Individuals Affected' is numeric
        data['Individuals Affected'] = pd.to_numeric(data['Individuals Affected'], errors='coerce')
        
        # Ensure 'Breach Submission Date' is a datetime type
        data['Breach Submission Date'] = pd.to_datetime(data['Breach Submission Date'])
        
        # Extract the year from the breach submission date
        data['Year'] = data['Breach Submission Date'].dt.year
        
        # Group by 'Year' and sum the number of individuals affected
        summary = data.groupby('Year', as_index=False)['Individuals Affected'].sum()
        summary = summary.rename(columns={'Individuals Affected': 'Total Individuals Affected'})
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def summarize_individuals_affected_by_year_with_percent_increase(file_path):
    """
    Summarize the total number of individuals affected grouped by year,
    and calculate the percent increase compared to the previous year.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the sum of individuals affected per year and percent increase.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Ensure 'Individuals Affected' is numeric
        data['Individuals Affected'] = pd.to_numeric(data['Individuals Affected'], errors='coerce')
        
        # Ensure 'Breach Submission Date' is a datetime type
        data['Breach Submission Date'] = pd.to_datetime(data['Breach Submission Date'])
        
        # Extract the year from the breach submission date
        data['Year'] = data['Breach Submission Date'].dt.year
        
        # Group by 'Year' and sum the number of individuals affected
        summary = data.groupby('Year', as_index=False)['Individuals Affected'].sum()
        summary = summary.rename(columns={'Individuals Affected': 'Total Individuals Affected'})
        
        # Calculate percent increase compared to the previous year
        summary['Percent Increase'] = summary['Total Individuals Affected'].pct_change() * 100
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    
def summarize_breaches_by_location(file_path):
    """
    Summarize the number of breaches grouped by location of breached information.

    :param file_path: Path to the CSV file.
    :return: DataFrame with the count of breaches per location.
    """
    try:
        # Load the CSV into a DataFrame
        data = pd.read_csv(file_path)
        
        # Group by 'Location of Breached Information' and count the number of breaches
        summary = data['Location of Breached Information'].value_counts().reset_index()
        summary.columns = ['Location of Breached Information', 'Count']
        
        return summary
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def calculate_median_and_mean_impact(data):
    """
    Calculate the median and mean number of individuals affected per breach type and per year.
    """
    try:
        # Group by 'Type of Breach' and calculate median and mean
        breach_type_stats = data.groupby('Type of Breach')['Individuals Affected'].agg(['median', 'mean']).reset_index()

        # Group by 'Year' and calculate median and mean
        year_stats = data.groupby('Year')['Individuals Affected'].agg(['median', 'mean']).reset_index()

        return breach_type_stats, year_stats
    except Exception as e:
        print(f"Error in calculating median and mean impact: {e}")
        return None, None


def identify_outliers(data, threshold=1_000_000):
    """
    Identify breaches with exceptionally high numbers of individuals affected (greater than a given threshold).
    """
    try:
        outliers = data[data['Individuals Affected'] > threshold]
        return outliers
    except Exception as e:
        print(f"Error in identifying outliers: {e}")
        return None


def calculate_cumulative_impact(data):
    """
    Calculate the cumulative total of individuals affected over the dataset's timeframe.
    """
    try:
        total_affected = data['Individuals Affected'].sum()
        return pd.DataFrame([{'Cumulative Impact': total_affected}])
    except Exception as e:
        print(f"Error in calculating cumulative impact: {e}")
        return None


def analyze_correlation_between_method_and_impact(data):
    """
    Analyze which breach types lead to the highest average number of individuals affected.
    """
    try:
        breach_type_impact = data.groupby('Type of Breach')['Individuals Affected'].mean().reset_index()
        breach_type_impact = breach_type_impact.rename(columns={'Individuals Affected': 'Average Individuals Affected'})
        return breach_type_impact
    except Exception as e:
        print(f"Error in analyzing correlation between method and impact: {e}")
        return None


def analyze_method_trends(data):
    """
    Examine how breach methods have evolved over time by counting occurrences per year.
    """
    try:
        method_trends = data.groupby(['Year', 'Type of Breach']).size().reset_index(name='Count')
        return method_trends
    except Exception as e:
        print(f"Error in analyzing method trends: {e}")
        return None

def filter_data_exclude_year(data, year_to_exclude):
    """
    Filter out rows from the DataFrame where the 'Year' equals the specified year.

    :param data: Original DataFrame
    :param year_to_exclude: Year to exclude from the data
    :return: Filtered DataFrame
    """
    return data[data['Year'] != year_to_exclude]


def count_breaches_by_year(data):
    """
    Count the number of breaches grouped by year.

    :param data: DataFrame containing the breach data.
    :return: DataFrame with the count of breaches per year.
    """
    try:
        # Group by 'Year' and count the number of breaches
        breaches_per_year = data.groupby('Year').size().reset_index(name='Count of Breaches')
        return breaches_per_year
    except Exception as e:
        print(f"Error in counting breaches by year: {e}")
        return None
    
def count_breaches_by_year_with_percent_increase(data):
    """
    Count the number of breaches grouped by year and calculate the percent increase compared to the previous year.

    :param data: DataFrame containing the breach data.
    :return: DataFrame with the count of breaches per year and percent increase.
    """
    try:
        # Group by 'Year' and count the number of breaches
        breaches_per_year = data.groupby('Year').size().reset_index(name='Count of Breaches')
        
        # Calculate percent increase compared to the previous year
        breaches_per_year['Percent Increase'] = breaches_per_year['Count of Breaches'].pct_change() * 100
        
        return breaches_per_year
    except Exception as e:
        print(f"Error in counting breaches by year with percent increase: {e}")
        return None
    