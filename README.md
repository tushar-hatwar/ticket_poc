# Ticket Data POC Analysis Project

## Overview

The Ticket Data POC Analysis project aims to analyze and gain insights from ticket-related data, including event details, ticket sales, user preferences, and venue information. The project is designed to follow a data transformation pipeline, from data ingestion to data analysis, utilizing Delta Lake for efficient storage and processing.

## Medallion Architecture

In this Project we are following below Architecture

![medallion](https://github.com/tushar-hatwar/ticket_poc/assets/60131764/4a5e240e-54fc-4f49-9d69-ca670e63f47b)


## Project Steps

### Data Ingestion

Data is obtained from various sources and ingested into the data lake. In this project, data is extracted from a GitHub repository and stored in the landing zone, located at the path '/mnt(storage_acc_name)/landing/'.

### Bronze Layer

The data in the landing zone is processed and stored in the Bronze Delta Tables in the `Bronze_ticket_db` database. The Bronze layer represents raw or minimally processed ticket data. Bronze Delta Tables include tables such as `category_bronze`, `date_bronze`, `events_bronze`, `listings_bronze`, `sales_bronze`, `users_bronze`, and `venue_bronze`, each containing specific ticket-related information.

### Silver Layer

Data from the Bronze Delta Tables is transformed and loaded into the Silver Delta Tables in the `Silver_ticket_db` database. The Silver layer represents cleaned and curated data with improved data quality. Silver Delta Tables include tables such as `category_silver`, `date_silver`, `events_silver`, `listings_silver`, `sales_silver`, `users_silver`, and `venue_silver`.

### Gold Layer

Data from the Silver Delta Tables is further transformed and loaded into the Gold Delta Tables in the `Gold_ticket_db` database. The Gold layer represents a dimensional model for ticket-related data, suitable for analysis and reporting. Gold Delta Tables include tables such as `category_dim`, `date_dim`, `events_fact`, `listings_fact`, `sales_fact`, `users_dim`, and `venue_dim`, designed to support analytical queries.

### Data Validation and Quality Checks

Throughout the project, data quality checks are performed to ensure the accuracy and completeness of the data. Validations are conducted for null data and duplicates to maintain data integrity.

### Surrogate Key Generation

Surrogate keys are generated and added to dimension tables in the Gold layer to uniquely identify records.

### Merge Operations

Merge operations are used to efficiently handle updates and inserts when moving data from one layer to another, ensuring smooth data flow and consistency.

### Data Analysis

Once the data is in the Gold Delta Tables, it is ready for analysis. Various analytical queries can be performed to gain insights into ticket sales trends, user preferences, popular events, and venue performance.

### Visualization and Reporting

The project includes visualizations and reports to present key findings and insights in a user-friendly manner. Data visualization tools and techniques are employed to create informative charts and dashboards.

## Benefits of the Project

- Improved Data Quality: The project ensures high data quality through data validations and cleansing, leading to reliable insights.
- Enhanced Analysis: The dimensional model in the Gold layer facilitates efficient analysis, allowing stakeholders to make informed decisions.
- Real-time Data Availability: Delta Lake's transactional capabilities enable real-time data availability for timely analysis.
- Scalability: The project architecture supports scalability to handle large volumes of ticket data efficiently.

## Conclusion

The Ticket Data POC Analysis project demonstrates a comprehensive approach to process, transform, and analyze ticket-related data. By leveraging Delta Lake and adhering to best practices, the project ensures data reliability, accuracy, and availability for meaningful insights and decision-making in the ticketing industry.

For more details, please refer to the [Jupyter Notebook](/path/to/your/notebook.ipynb) containing the code implementation and analysis.

---
_This project description is written based on the information provided by the user. If there are any additional details or modifications required, please feel free to update the description accordingly._
