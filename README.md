# Flight Analysis with Spark and Scala

This project performs various analyses on flight and passenger data using Apache Spark and Scala, following a functional programming approach.

## Prerequisites

- Java 8 (JDK version 1.8)
- Scala 2.12.10
- Apache Spark 2.4.8

## Steps to Complete the Assignment

1. **Set up the Project**: Create a new Scala project in IntelliJ IDEA.

2. **Define Case Classes**: Create separate case classes for input and output data:
   - `FlightData`
   - `Passenger`
   - `FlightsByMonth`
   - `FrequentFlyer`
   - `LongestRun`
   - `FlightsTogether`
   - `FlightsInRange`

3. **Implement Data Analysis Functions**: Implement the following functions to perform the required analyses:
   - `readFlightData`
   - `readPassengerData`
   - `findFlightsByMonth`
   - `findFrequentFlyers`
   - `findLongestNonUKRun`
   - `findFlightsTogether`
   - `findFlightsWithinRange`

4. **Write Output to CSV Files**: Write the output of each analysis to separate CSV files in the project root directory, with a single CSV file per output and no partitioning.

5. **Ensure Functional Programming Principles**: Follow functional programming principles throughout the implementation:
   - Use immutable values
   - Modularize code with functions
   - Work with typed Datasets instead of DataFrames whenever possible

6. **Add Documentation**: Document the code using ScalaDocs for each function, explaining its purpose, parameters, and return values.

7. **Naming Conventions**: Follow camelCase naming conventions for variables and functions.

## Running the Code in IntelliJ

1. **Set up Spark and Scala Versions**:
   - In IntelliJ IDEA, go to `File` > `Project Structure` > `Project Settings` > `Libraries`.
   - Click on the `+` button and select `From Maven...`.
   - Search for `org.apache.spark:spark-core_2.12:2.4.8` and add it to the project.
   - Repeat the same process for `org.apache.spark:spark-sql_2.12:2.4.8`.

2. **Set up JDK Version**:
   - In IntelliJ IDEA, go to `File` > `Project Structure` > `Project Settings` > `Project`.
   - Under the `Project SDK` section, select the appropriate JDK version (1.8) from the dropdown menu or configure a new JDK if needed.

3. **Run the Application**:
   - In the `Main.scala` file, locate the `main` method.
   - Right-click on the method name and select `Run 'Main.main()'`.

4. **Output**:
   - The application will read the input data from the `data` directory.
   - The output CSV files will be generated in the project root directory.

Note: Make sure to replace the placeholders in the code (e.g., file paths) with the appropriate values for your setup.
