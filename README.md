# Naya: Cloud Data Engineering - Prices

## 📘 Description
<p>This repository automates the daily scraping of data and provides an efficient pipeline for storage, analysis, and visualization.</p>

<ul>
  <li><strong>Data Scraping:</strong> Automatically scrapes data daily to ensure fresh and updated information.</li>
  <li><strong>Data Storage:</strong> Stores scraped data in a MinIO bucket, making it accessible to analysts via Trino and Hive Metastore.</li>
  <li><strong>Data Streaming:</strong> 
    <ul>
      <li>Publishes data to Kafka streams, enabling providers and consumers to process new data related to stores and prices.</li>
    </ul>
  </li>
  <li><strong>Data Consumption:</strong>
    <ul>
      <li>Prepares data for analysis, including cleaning and generating new columns for machine learning purposes.</li>
      <li>Powers <em>Power BI Dashboards</em> for insights and visualization.</li>
      <li>Supports a 7-day monitoring window for specific use cases.</li>
    </ul>
  </li>
  <li><strong>Data Archival:</strong> Automatically checks for data older than 7 days and moves it to an archive for long-term storage.</li>
</ul>

<p>This end-to-end pipeline ensures seamless data management and usability for various business and analytical needs.</p>


## 🛠️ Tech Stack

<div align="center">
    <!-- Python -->
    <img src="https://www.vectorlogo.zone/logos/python/python-horizontal.svg" alt="Python" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- PySpark -->
    <img src="https://www.vectorlogo.zone/logos/apache_spark/apache_spark-ar21.svg" alt="PySpark" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- Kafka -->
    <img src="https://www.vectorlogo.zone/logos/apache_kafka/apache_kafka-ar21.svg" alt="Kafka" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- PostgreSQL -->
    <img src="https://www.vectorlogo.zone/logos/postgresql/postgresql-horizontal.svg" alt="PostgreSQL" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- MinIO -->
    <img src="https://www.vectorlogo.zone/logos/minioio/minioio-ar21.svg" alt="MinIO" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- Linux -->
    <img src="https://www.vectorlogo.zone/logos/linux/linux-ar21.svg" alt="Linux" height="60">
    &nbsp;&nbsp;&nbsp;&nbsp;
    <!-- Trino -->
    <img src="https://upload.wikimedia.org/wikipedia/commons/5/57/Trino-logo-w-bk.svg" alt="Trino" height="60">
</div>

## 🏗️ Architecture

<div align="center">
    <img src="https://i.imgur.com/aAN7hIs.png" alt="Architecture Diagram" width="850">
</div>

## 👥 Authors

- **Zvi Amosi**  
  <a href="https://www.linkedin.com/in/tzvizamosy/" target="_blank">
    <img src="https://seeklogo.com/images/L/linkedin-logo-920846F1F7-seeklogo.com.png" alt="LinkedIn" width="65" style="vertical-align:middle; margin-left:35px;">
  </a>

- **Alexey Serdtse**  
  <a href="https://www.linkedin.com/in/alexey-serdtse/" target="_blank">
    <img src="https://seeklogo.com/images/L/linkedin-logo-920846F1F7-seeklogo.com.png" alt="LinkedIn" width="65" style="vertical-align:middle; margin-left:35px;">
  </a>
