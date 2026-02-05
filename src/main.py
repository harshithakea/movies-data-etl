import pandas as pd

def main():
	print("Starting Movies ETL Pipeline")
	data = {
		"movie_id":[1,2,3],
		"movie":['Hello', 'Hi Nanna', 'King'],
		"quantity":[2,6,3],
		"price":[120,150,200]
	}
	df = pd.DataFrame(data)
	df["total_amount"]=df["quantity"]*df["price"]

	print(df)
	output_path = "/home/ubuntu/movies-data-etl/data/processed/movies_processed.csv"
	df.to_csv(output_path, index=False)

	print(f"Processed data saved to {output_path}")
if __name__ == "__main__":
	main()

