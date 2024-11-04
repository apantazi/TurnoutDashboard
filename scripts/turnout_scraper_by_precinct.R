if (!requireNamespace('sf', quietly = TRUE)) install.packages('sf', repos='http://cran.rstudio.com/')
if (!requireNamespace('httr', quietly = TRUE)) install.packages('httr', repos='http://cran.rstudio.com/')
if (!requireNamespace('jsonlite', quietly = TRUE)) install.packages('jsonlite', repos='http://cran.rstudio.com/')
if (!requireNamespace('tidyverse', quietly = TRUE)) install.packages('tidyverse', repos='http://cran.rstudio.com/')
if (!requireNamespace('scales', quietly = TRUE)) install.packages('scales', repos='http://cran.rstudio.com/')

library(httr)
library(jsonlite)
library(tidyverse)
library(scales)
library(sf)
log_message <- function(message) {
  log_entry <- paste(Sys.time(), "-", message)
  cat(log_entry, "\n")  # Outputs to console for GitHub Actions logging
}

# Function to fetch and process data
fetch_and_process_data <- function() {
  log_message <- function(message) {
    log_entry <- paste(Sys.time(), "-", message)
    cat(log_entry, "\n")  # Outputs to console for GitHub Actions logging
  }
  
  log_message("Script started.")
  
  county_data <- read_csv("https://raw.githubusercontent.com/apantazi/TurnoutScraper/main/data/Turnout_Scraping_Codes.csv")
  
  turnout_list <- list()
  for (i in seq_along(county_data$turnout_data_link)) {
    response <- tryCatch(
      httr::GET(county_data$turnout_data_link[i]),
      error = function(e) {
        log_message(paste("Network error for link:", county_data$turnout_data_link[i]))
        NULL
      }
    )
    
    if (inherits(response, "try-error")) {
      log_message(paste("Error fetching data for link:", county_data$turnout_data_link[i]))
      next
    }
    if (response$status_code == 200) {
      json_data <- content(response, as = "text", encoding = "UTF-8")
      turnout_list[[i]] <- fromJSON(json_data)
    } else {
      log_message(paste("Failed to fetch data for link:", county_data$turnout_data_link[i], "Status code:", response$status_code))
    }
  }
  
  county_precinct_turnout <- data.frame()
  
  for (i in seq_along(turnout_list)) {
    county_name <- county_data$`County Code`[i]
    
    # Process turnout by PrecinctType
    precinct_turnout <- bind_rows(lapply(names(turnout_list[[i]]$Turnout$PrecinctType), function(precinct) {
      precinct_data <- turnout_list[[i]]$Turnout$PrecinctType[[precinct]]
      
      # Use `[[ ]]` to check for each turnout type, setting to 0 if not found
      early_voting <- precinct_data$BallotTypeTotals[["EarlyVoting"]] %||% 0
      mail <- precinct_data$BallotTypeTotals[["Mail"]] %||% 0
      election_day <- precinct_data$BallotTypeTotals[["ElectionDay"]] %||% 0
      provisional <- precinct_data$BallotTypeTotals[["Provisional"]] %||% 0
      
      total_turnout <- early_voting + mail + election_day + provisional
      
      voters <- precinct_data$EligibleVoters
      turnout_rate <- total_turnout/voters
      
      data.frame(
        County = county_name,
        Precinct = precinct,
        Turnout = total_turnout,
        Registered_Voters = voters,
        turnout_rate = turnout_rate,
        County_Precinct = str_trim(paste0(str_trim(county_name),str_trim(precinct)))
      )
    }))
    
    county_precinct_turnout <- bind_rows(county_precinct_turnout, precinct_turnout)
  }
  
  log_message("Data processing completed successfully.")
  
  return(county_precinct_turnout)
} 
  

# Execute the function and store the result in a DataFrame
precinct_turnout_data <- fetch_and_process_data()

## read in voter file ####
#voters <- read_delim("C:/Users/Andrew/Desktop/20240813_VoterDetail/combine.txt",  delim = '\t',escape_double = FALSE, trim_ws = TRUE, col_names=c("County", "Voter_ID", "Name_Last", "Name_Suffix", "Name_First", "Name_Middle", "PRR_exempt", "Residence_1", "Residence_2", "Residence_City", "Residence_State", "Residence_Zip", "Mailing_Address_1", "Mailing_Address_2", "Mailing_Address_3", "Mailing_City", "Mailing_State", "Mailing_Zip", "Mailing_Country", "Sex", "Race", "DOB", "Reg_Date", "Party", "Precinct", "Precinct_Group", "Precinct_Split", "Precinct_Suffix", "Voter_Status", "Congressional_District", "House_District", "Senate_District", "County_Commission_District", "School_Board_District", "Area_Code", "Phone", "Phone_ext", "Email"),col_types = cols(.default = "c"),col_select = c("County", "Voter_ID", "Party", "Precinct_Split","Precinct","Precinct_Group","Precinct_Suffix", "Congressional_District", "House_District", "Senate_District"))
#saveRDS(voters, "voters.RDS")
#system("aws s3 cp C:/Users/Andrew/Documents/turnout_scraper/voters.rds s3://data.jaxtrib.org/turnout_data/voters.rds")
log_message("Reading voters.RDS from AWS S3...")
voters <- readRDS(url("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/voters.RDS"))


## ####
## read in improved crosswalk ####
log_message("Reading precinct_assignment.csv from AWS S3...")

#precinct_assignment <- read_csv("precinct_assignment.csv",col_types = "ccccccn")
#system("aws s3 cp C:/Users/Andrew/Documents/turnout_scraper/precinct_assignment.csv s3://data.jaxtrib.org/turnout_data/precinct_assignment.csv")
precinct_assignment <- read_csv(url("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/precinct_assignment.csv"),col_types=c("ccccccn"))

precinct2 <- left_join(precinct_turnout_data,precinct_assignment,by="County_Precinct") %>% 
  mutate(votes = Turnout * Pct)
## ####

#system("aws s3 cp C:/Users/Andrew/Downloads/florida_state_house_districts_2024.geojson s3://data.jaxtrib.org/turnout_data/")
#system("aws s3 cp C:/Users/Andrew/Downloads/fl_state_senate_districts_2024.geojson s3://data.jaxtrib.org/turnout_data/")
#system("aws s3 cp C:/Users/Andrew/Downloads/FL_congressional_districts_2024.geojson s3://data.jaxtrib.org/turnout_data/")
#system("aws s3 cp C:/Users/Andrew/Documents/HouseDistrict-Bookclosing-2024.csv s3://data.jaxtrib.org/turnout_data/HouseDistrict-Bookclosing-2024.csv")
#system("aws s3 cp C:/Users/Andrew/Documents/SD_bookclosing.csv s3://data.jaxtrib.org/turnout_data/SD_bookclosing.csv")
#system("aws s3 cp C:/Users/Andrew/Documents/CongressionalDistrict-Bookclosing.csv s3://data.jaxtrib.org/turnout_data/CongressionalDistrict-Bookclosing.csv")

## create district DFs with turnout data ####
HD <- precinct2 %>% group_by(House_Districts) %>% summarize(votes=sum(votes)) %>% 
  left_join(read_csv("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/HouseDistrict-Bookclosing-2024.csv",col_types = "ci")) %>% 
  mutate(turnout=votes/Voters) %>% arrange(desc(turnout)) %>% 
  left_join(sf::read_sf("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/florida_state_house_districts_2024.geojson"),by=c("House_Districts"="NAME")) %>% 
  sf::st_as_sf() %>%  st_make_valid() %>%   st_cast(., "MULTIPOLYGON") %>%
  st_transform(.,crs = 4326) %>% filter(!is.na(House_Districts)) %>% 
  mutate(tooltip = paste0("District ", House_Districts, " Turnout: ", scales::percent(turnout,accuracy=0.01))) %>% 
  filter(!grepl(",",House_Districts))

SD <- precinct2 %>% group_by(Senate_Districts) %>% summarize(votes=sum(votes)) %>%
  left_join(read_csv("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/SD_bookclosing.csv",col_types="ci")) %>% 
  mutate(turnout=votes/Voters) %>% arrange(desc(turnout))%>% 
  left_join(sf::read_sf("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/fl_state_senate_districts_2024.geojson"),by=c("Senate_Districts"="NAME"))%>% 
  sf::st_as_sf() %>%  st_make_valid() %>%   st_cast(., "MULTIPOLYGON") %>%
  st_transform(.,crs = 4326) %>% filter(!is.na(Senate_Districts)) %>% 
  mutate(tooltip = paste0("District ", Senate_Districts, " Turnout: ", scales::percent(turnout,accuracy=0.01))) %>% 
  filter(!grepl(",",Senate_Districts))

CD <- precinct2 %>% mutate(Congressional_Districts = as.character(Congressional_Districts)) %>% group_by(Congressional_Districts) %>% summarize(votes=sum(votes)) %>%
  left_join(read_csv("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/CongressionalDistrict-Bookclosing.csv",col_types="ci")) %>% 
  mutate(turnout=votes/Voters) %>% arrange(desc(turnout)) %>% 
  left_join(sf::read_sf("https://s3.amazonaws.com/data.jaxtrib.org/turnout_data/FL_congressional_districts_2024.geojson"),by=c("Congressional_Districts"="NAME"))%>% 
  sf::st_as_sf() %>%  st_make_valid() %>%   st_cast(., "MULTIPOLYGON") %>%
  st_transform(.,crs = 4326) %>% filter(!is.na(Congressional_Districts)) %>% 
  mutate(tooltip = paste0("District ", Congressional_Districts, " Turnout: ", scales::percent(turnout,accuracy=0.01))) %>% 
  filter(!grepl(",",Congressional_Districts))
## ####

# File paths for RDS files
output_file_cd <- "CD_turnout_data.RDS"
output_file_hd <- "HD_turnout_data.RDS"
output_file_sd <- "SD_turnout_data.RDS"

# Save the data frames as RDS files
tryCatch({
  saveRDS(CD, output_file_cd)
  saveRDS(HD, output_file_hd)
  saveRDS(SD, output_file_sd)
  log_message("Successfully wrote RDS files.")
  
  # Upload each file to S3
  system("aws s3 cp CD_turnout_data.RDS s3://data.jaxtrib.org/turnout_data/CD_turnout_data.RDS")
  system("aws s3 cp HD_turnout_data.RDS s3://data.jaxtrib.org/turnout_data/HD_turnout_data.RDS")
  system("aws s3 cp SD_turnout_data.RDS s3://data.jaxtrib.org/turnout_data/SD_turnout_data.RDS")
  log_message("Successfully uploaded RDS files to S3.")
}, error = function(e) {
  log_message(paste("Error writing to RDS:", e$message))
})
