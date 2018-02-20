# to get the JQL functions
getJqlFunctions <- function(property,format) {
  if (format == 'str'){
    functions <- paste0(
      "function ",property,"_na(item){return _.isUndefined(item.properties.",property,");}",
      "function ",property,"_format(item){return _.isString(item.properties.",property,");}"
    )
  } else if (format == 'num') {
    functions <- paste0(
      "function ",property,"_na(item){return _.isUndefined(item.properties.",property,");}",
      "function ",property,"_format(item){return _.isNumber(item.properties.",property,");}"
    )
  } else if (format == 'int') {
    functions <- paste0(
      "function ",property,"_na(item){return _.isUndefined(item.properties.",property,");}",
      "function ",property,"_format(item){return _.isNumber(item.properties.",property,");}"
    )
  } else if (format == 'array') {
    functions <- paste0(
      "function ",property,"_na(item){return _.isUndefined(item.properties.",property,");}",
      "function ",property,"_format(item){return _.isArray(item.properties.",property,");}"
    )
  } else if (format == 'json') {
    functions <- paste0(
      "function ",property,"_na(item){return _.isUndefined(item.properties.",property,");}",
      "function ",property,"_format(item){return _.isObject(item.properties.",property,");}"
    )
  }
}

# to make the groupby
