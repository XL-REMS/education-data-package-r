# retrieve results from all pages of an api query for a single year
#
# returns data.frame, or an error if query fails
get_year_data <- function(url) {
  message('\nFetching data for ', substring(url, 41), ' ...')
  request <- httr::GET(url)

  if (request$status_code != 200) {
    stop('Query page not found.\n',
         'Please double-check your arguments (especially filters).\n',
         'Consider filing an issue with the development team if this issue persists.',
         call. = FALSE)
  }

  resp <- jsonlite::fromJSON(rawToChar(request$content))
  expected_rows <- resp$count

  if (expected_rows == 0) {
    warning('Query ', url, ' returned no results.', call. = FALSE)
    df <- data.frame()
    return(df)
  }

  pages <- ceiling(expected_rows / nrow(resp$results))
  dfs <- vector('list', pages)
  count = 1

  dfs[[count]] <- resp$results
  url <- resp[['next']]

  while (!(is.null(url))) {
    count = count + 1
    message(paste("Processing page", count, 'out of', pages))
    request <- httr::GET(url)

    if (request$status_code != 200) {
      stop('Query page not found.\n',
           'Please double-check your arguments (especially filters).\n',
           'Consider filing an issue with the development team if this issue persists.',
           call. = FALSE)
    }

    resp <- jsonlite::fromJSON(rawToChar(request$content))
    dfs[[count]] <- resp$results
    url <- resp[['next']]
  }

  df <- do.call(rbind, dfs)

  if (nrow(df) != expected_rows) {
    warning('API call expected ', expected_rows, ' results but received ',
            nrow(df), '. Consider filing an issue with the development team.',
            call. = FALSE)
  }

  return(df)
}

# retrieve results from all pages of an api query across all given years
#
# returns data.frame
get_all_data <- function(urls, parallel) {
  ncores <- parallel::detectCores(logical = FALSE)
  if (parallel & (ncores > 1)) {
    message('Retrieving data in parallel across ', ncores, ' cores...')
    cl <- parallel::makeCluster(ncores)
    dfs <- parallel::parLapplyLB(cl, urls, get_year_data)
    parallel::stopCluster(cl)
  } else {
    dfs <- lapply(urls, get_year_data)
  }
  #cl <- parallel::makeCluster(6)
  #dfs <- parallel::parLapplyLB(cl, urls, get_year_data)
  #parallel::stopCluster(cl)
  #future::plan(sequential)
  df <- do.call(rbind, dfs)
  return(df)
}
