# Flight Data Analysis with Apache Spark and Scala

This project is a solution to the Flight Data Coding Assignment from Quantexa. It performs various analyses on flight and passenger data using Apache Spark and Scala, following a functional programming approach.

## Assignment Instructions

The assignment instructions provided the following requirements:

1. **Data**: Two CSV files were provided: `flightData.csv` and `passengers.csv`.

2. **Questions**:
    - Question 1: Find the total number of flights for each month.
    - Question 2: Find the names of the 100 most frequent flyers.
    - Question 3: Find the greatest number of countries a passenger has been in without being in the UK.
    - Question 4: Find the passengers who have been on more than 3 flights together.
    - Extra Marks: Find the passengers who have been on more than N flights together within a given date range.

3. **Requirements**:
    - The solution should be written in Spark/Scala, following a functional programming style.
    - The output can be provided as separate files or printed to the console.
    - The solution should be correct, performant, and follow good coding practices, including documentation and ease of usage.

## Steps to Complete the Assignment

1. **Set up the Project**: Create a new Scala project in IntelliJ IDEA and set up the required dependencies (Apache Spark, Scala).

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
    - `findFlightsByMonth` (for Question 1)
        - Extract the month from the date column
        - Group by month and count the number of flights
        - Return dataset of (month, numFlights)
    - `findFrequentFlyers` (for Question 2)
        - Join flightData and passengers datasets on passengerId
        - Group by passengerId and count flights per passenger
        - Join result with passengers to get name columns
        - Order by number of flights descending
        - Take top 100 results
        - Return dataset of (passengerId, numFlights, firstName, lastName)
    - `findLongestNonUKRun` (for Question 3)
        - Group flights by passengerId and collect to a list of flights
        - For each passengerId, walk the flights list and track current sequence length and longest sequence seen
        - Reset current sequence to 0 when "UK" is encountered
        - After processing all flights, take the longest sequence value
        - Return dataset of (passengerId, longestSequenceLength)
        - Order by longestSequenceLength descending
    - `findFlightsTogether` (for Question 4)
        - Self-join flightData on flightId where passengerId1 != passengerId2
        - Group by (passengerId1, passengerId2) and count matching flights
        - Filter for counts > 3
        - Return dataset of (passengerId1, passengerId2, numberOfFlightsTogether)
        - Order by numberOfFlightsTogether descending
    - `findFlightsWithinRange` (for Extra Marks)

4. **Write Output to CSV Files**: Write the output of each analysis to separate CSV files in the project root directory, with a single CSV file per output and no partitioning.

5. **Ensure Functional Programming Principles**: Follow functional programming principles throughout the implementation:
    - Use immutable values
    - Modularize code with functions
    - Work with typed Datasets instead of DataFrames whenever possible

6. **Add Documentation**: Document the code using ScalaDocs for each function, explaining its purpose, parameters, and return values.

7. **Naming Conventions**: Follow camelCase naming conventions for variables and functions.

8. **Testing**: While not explicitly required, consider adding unit tests for the implemented functions to ensure their correctness.

## Code Review

The provided code follows the assignment instructions and adheres to functional programming principles:

- **Case Classes**: Separate case classes are defined for input and output data, allowing for type safety and immutability.
- **Modular Functions**: The code is modularized into separate functions, each responsible for a specific analysis task.
- **Typed Datasets**: The code works with typed Datasets for both input and output data, as recommended.
- **Immutable Values**: All values in the code are immutable, using `val` instead of `var`.
- **Naming Conventions**: The code follows camelCase naming conventions for variables and functions.
- **Documentation**: ScalaDocs are provided for each function, explaining their purpose, parameters, and return values.
- **Output to CSV Files**: The output of each analysis is written to separate CSV files in the project root directory, with no partitioning.

The code also includes instructions for running it in IntelliJ with the specified versions of Spark, Scala, and JDK.

## Running the Code

Follow these steps to run the code in IntelliJ IDEA:

1. **Set up IntelliJ IDEA**:
   - Install IntelliJ IDEA Community Edition if you haven't already.
   - Open IntelliJ IDEA and create a new Scala project.

2. **Set up Spark and Scala Versions**:
   - In IntelliJ IDEA, go to `File` > `Project Structure` > `Project Settings` > `Libraries`.
   - Click on the `+` button and select `From Maven...`.
   - Search for `org.apache.spark:spark-core_2.12:2.4.8` and add it to the project.
   - Repeat the same process for `org.apache.spark:spark-sql_2.12:2.4.8`.

3. **Set up JDK Version**:
   - In IntelliJ IDEA, go to `File` > `Project Structure` > `Project Settings` > `Project`.
   - Under the `Project SDK` section, select the appropriate JDK version (1.8) from the dropdown menu or configure a new JDK if needed.

4. **Copy the Project Files**:
   - Copy the `Main.scala` file and the case class files (`FlightData.scala`, `Passenger.scala`, `FlightsByMonth.scala`, `FrequentFlyer.scala`, `LongestRun.scala`, `FlightsTogether.scala`, and `FlightsInRange.scala`) to the appropriate location in your IntelliJ project.

5. **Prepare the Input Data**:
   - Create a new directory named `data` in the project root directory.
   - Place the input CSV files (`flightData.csv` and `passengers.csv`) in the `data` directory.

6. **Create the Output Directory**:
   - Create a new directory named `Output` in the project root directory. This is where the output CSV files will be generated.

7. **Run the Application**:
   - In the `Main.scala` file, locate the `main` method.
   - Right-click on the method name and select `Run 'Main.main()'`.

The application should now run successfully, and the output CSV files will be generated in the `Output` directory within your project.

Note:
- Make sure to replace the placeholders in the code (e.g., file paths) with the appropriate values for your setup.
- If you encounter any issues during the setup or run, refer to the Apache Spark and Scala documentation for additional guidance.

## Testing

While the provided code does not include unit tests, it would be a good practice to add unit tests for the implemented functions to ensure their correctness and facilitate future code changes.