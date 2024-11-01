library(googlesheets4)
library(httr)
library(jsonlite)
library(tidyverse)
library(scales)
library(lubridate)

# Function to fetch and process data
fetch_and_process_data <- function() {
  
  log_file <- "C:/Users/Andrew/Documents/turnout_scraper_new/log.txt"
  log_message <- function(message) {
    write(paste(Sys.time(), message), file = log_file, append = TRUE)
  }
  
  log_message("Script started.")
  
  county_data <- read_csv("https://raw.githubusercontent.com/apantazi/TurnoutScraper/main/data/Turnout_Scraping_Codes.csv")
  #sheet_url <- "https://docs.google.com/spreadsheets/d/1dpaQ1PySOvKz-7hv3s9UBCu_atkD6xovwaqAb4tmQSA/edit?usp=sharing"
  #county_data <- googlesheets4::read_sheet(sheet_url)
  #2
  
  turnout_list <- list()
  for (i in seq_along(county_data$turnout_data_link)) {
    response <- try(httr::GET(county_data$turnout_data_link[i]), silent = TRUE)
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
  
  county_overall_party_turnout <- data.frame()
  county_registered_voters <- data.frame()
  
  for (i in seq_along(turnout_list)) {
    county_name <- county_data$County[i]
    if (is.null(turnout_list[[i]]$Summary$TotalRegisteredVoters)) next
    total_registered_voters <- turnout_list[[i]]$Summary$TotalRegisteredVoters
    county_registered_voters <- bind_rows(county_registered_voters,
                                          data.frame(County = county_name,
                                                     TotalRegisteredVoters = total_registered_voters))
    
    overall_party_turnout <- bind_rows(lapply(names(turnout_list[[i]]$Turnout$PartyType), function(party) {
      data.frame(
        Party = party,
        Total = sum(unlist(turnout_list[[i]]$Turnout$PartyType[[party]]))
      )
    }))
    if (nrow(overall_party_turnout) > 0) {
      county_overall_party_turnout <- bind_rows(county_overall_party_turnout,
                                                data.frame(County = county_name, overall_party_turnout))
    }
  }
  
  combined_party_turnout <- county_overall_party_turnout %>%
    mutate(Party = case_when(
      Party %in% c("REP", "DEM") ~ Party,
      TRUE ~ "OTHER"
    )) %>%
    group_by(County, Party) %>%
    summarize(Total = sum(Total), .groups = 'drop') %>% 
    pivot_wider(names_from = Party, values_from = Total, values_fill = 0) %>% 
    mutate(Total = DEM + REP + OTHER, 
           DEM_Percent = (DEM / Total), 
           REP_Percent = (REP / Total), 
           OTHER_Percent = (OTHER / Total))
  
  # Calculate turnout rate
  county_turnout_rate <- county_registered_voters %>%
    left_join(combined_party_turnout %>%
                group_by(County) %>%
                summarize(TotalVotes = sum(Total)),
              by = "County") %>%
    mutate(TurnoutRate = (TotalVotes / TotalRegisteredVoters))
  
  # Combine turnout rate with the party turnout data
  final_data <- combined_party_turnout %>%
    left_join(county_turnout_rate %>%
                select(County, TotalVotes,TurnoutRate,TotalRegisteredVoters), by = "County")
  
  # Calculate statewide totals
  statewide_totals <- final_data %>%
    summarize(across(c(DEM, REP, OTHER, Total,TotalVotes,TotalRegisteredVoters), sum)) %>%
    mutate(
      County = "Statewide",
      DEM_Percent = DEM / Total,
      REP_Percent = REP / Total,
      OTHER_Percent = OTHER / Total,
      TurnoutRate = sum(TotalVotes) / sum(TotalRegisteredVoters)
    )
  
  # Bind statewide totals to the top of the table
  final_data <- bind_rows(statewide_totals, final_data)
  final_data$Last_Updated <- format(with_tz(now(), "America/New_York"), "%Y-%m-%d %H:%M:%S")
  
  # Attempt to write to CSV
  output_file <- "C:/Users/Andrew/Documents/turnout_scraper_new/combined_party_turnout.csv"
  tryCatch({
    write_csv(final_data, output_file)
    log_message("Successfully wrote to CSV.")
    # Upload to S3
    system("aws s3 cp C:/Users/Andrew/Documents/turnout_scraper_new/combined_party_turnout.csv s3://data.jaxtrib.org/turnout_data/combined_party_turnout.csv")
    log_message("Successfully uploaded to S3.")
  }, error = function(e) {
    log_message(paste("Error writing to CSV:", e$message))
  })
  
  return(final_data)
} 

# Execute the function
data <- fetch_and_process_data()
