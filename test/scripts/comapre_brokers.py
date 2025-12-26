#!/usr/bin/env python3  
import pandas as pd  
import matplotlib.pyplot as plt  
import argparse  
  
def load_csv_data(filepath):  
    try:  
        df = pd.read_csv(filepath)  
        return df  
    except FileNotFoundError:  
        print(f"Warning: {filepath} not found")  
        return None  
  
def create_comparison_plot(walrus_df, kafka_df, gobroker_df, output_file):  
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))  
  
    systems = []  
    throughputs = []  
      
    if walrus_df is not None:  
        systems.append('Walrus')  
        throughputs.append(walrus_df['writes_per_second'].mean())  
      
    if kafka_df is not None:  
        systems.append('Kafka')  
        throughputs.append(kafka_df['writes_per_second'].mean())  
          
    if gobroker_df is not None:  
        systems.append('cursus')  
        throughputs.append(gobroker_df['writes_per_second'].mean())  
      
    ax1.bar(systems, throughputs, color=['orange', 'blue', 'green'])  
    ax1.set_title('Writes Per Second Comparison')  
    ax1.set_ylabel('Writes/Second')  
 
    bandwidths = []  
      
    if walrus_df is not None:  
        bandwidths.append(walrus_df['bytes_per_second'].mean() / (1024*1024))  # MB/s  
      
    if kafka_df is not None:  
        bandwidths.append(kafka_df['bytes_per_second'].mean() / (1024*1024))  
          
    if gobroker_df is not None:  
        bandwidths.append(gobroker_df['bytes_per_second'].mean() / (1024*1024))  
      
    ax2.bar(systems, bandwidths, color=['orange', 'blue', 'green'])  
    ax2.set_title('Bandwidth Comparison')  
    ax2.set_ylabel('MB/Second')  
      
    plt.tight_layout()  
    plt.savefig(output_file, dpi=300, bbox_inches='tight')  
    plt.show()  
  
def print_summary(walrus_df, kafka_df, gobroker_df):  
    print("\n" + "="*60)  
    print("BENCHMARK COMPARISON SUMMARY")  
    print("="*60)  
      
    print("\nAverage Throughput & Bandwidth:")  
      
    if walrus_df is not None:  
        avg_writes = walrus_df['writes_per_second'].mean()  
        avg_bytes = walrus_df['bytes_per_second'].mean() / (1024*1024)  
        print(f"  Walrus:  Avg {avg_writes:,.0f} writes/s ({avg_bytes:.2f} MB/s)")  
      
    if kafka_df is not None:  
        avg_writes = kafka_df['writes_per_second'].mean()  
        avg_bytes = kafka_df['bytes_per_second'].mean() / (1024*1024)  
        print(f"  Kafka:   Avg {avg_writes:,.0f} writes/s ({avg_bytes:.2f} MB/s)")  
          
    if gobroker_df is not None:  
        avg_writes = gobroker_df['writes_per_second'].mean()  
        avg_bytes = gobroker_df['bytes_per_second'].mean() / (1024*1024)  
        print(f"  Cursus: Avg {avg_writes:,.0f} writes/s ({avg_bytes:.2f} MB/s)")  
  
def main():  
    parser = argparse.ArgumentParser(description='Compare broker benchmark results')  
    parser.add_argument('--walrus', required=True, help='Walrus CSV file')  
    parser.add_argument('--kafka', required=True, help='Kafka CSV file')  
    parser.add_argument('--gobroker', required=True, help='cursus CSV file')  
    parser.add_argument('--output', default='comparison.png', help='Output image file')  
      
    args = parser.parse_args()  
       
    walrus_df = load_csv_data(args.walrus)  
    kafka_df = load_csv_data(args.kafka)  
    gobroker_df = load_csv_data(args.gobroker)  

    create_comparison_plot(walrus_df, kafka_df, gobroker_df, args.output)  

    print_summary(walrus_df, kafka_df, gobroker_df)  
  
if __name__ == "__main__":  
    main()