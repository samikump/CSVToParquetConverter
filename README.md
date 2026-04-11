## CSV to Parquet Converter

A Java application designed to convert CSV files into Apache Parquet format.

### But Why?

Nowadays, many companies and organizations share their data in CSV format. However, the Apache Parquet format is often superior for several reasons:
- **Better Compression:** Since Parquet stores data columnarly, similar data is grouped together, resulting in much higher compression ratios.
- **Type Safety:** Parquet preserves data types (doubles, integers, strings, dates), whereas CSV treats everything as text.
- **Performance:** Parquet offers significantly faster read performance by loading only the necessary columns.

### How it Works

1. **Input:** The application prompts the user to select a CSV file.
2. **Validation:** It validates the CSV structure before starting the conversion.
3. **In-memory Check:** After conversion, it performs an in-memory validation of the generated Parquet data.
4. **Output:** If the validation passes, the Parquet file is saved in the same directory as the source. 
   - *Note: If a file with the same name already exists, the user is prompted to overwrite it.*
5. **Results:** Finally, the application displays the file sizes and the calculated compression percentage.

### Testing

I tested the application using the **City of Helsinki Purchase Dataset (2017)** from *avoindata.fi*. The result demonstrates the efficiency of the conversion:

![Conversion Example](images/success_example.png)
