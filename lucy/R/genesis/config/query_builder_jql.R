queries_list <- list(
  mixpanel = list(
    main = paste0("function main() {return Events({from_date: '",from_date,"',to_date:   '",to_date,"'})"),
    dsk = paste(c(
      ".filter(e=>e.properties.mp_lib==='web')",
      ".filter(e=>_.isUndefined(e.properties.$current_url)===false)",
      ".filter(e=>e.properties.$current_url.includes('fixeads')===false)",
      ".filter(e=>e.properties.$device===undefined)"
    ), collapse = ''),
    rwd = paste(c(
      ".filter(e=>e.properties.mp_lib==='web')",
      ".filter(e=>_.isUndefined(e.properties.$current_url)===false)",
      ".filter(e=>e.properties.$current_url.includes('fixeads')===false)",
      ".filter(e=>e.properties.$device!==undefined)"
    ), collapse = ''),
    and = ".filter(e=>e.properties.mp_lib==='android')",
    ios = ".filter(e=>e.properties.mp_lib==='iphone')",
    cars = list(
      poland = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/oferta/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.otomoto.pl/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$cars$poland$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[3])===true)"),
        multipay = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/nowe-ogloszenie/')===true).filter(e=>e.name.includes('/')===false)",
        seller = ".filter(e=> _.contains(['otomoto','www'],e.properties.$current_url.split('/')[2].split('.')[0])===false)",
        credentials = list(
          project_name = 'otomoto.pl',
          token = 'b2b9c69bb88736c7e833e9d609004e6a',
          key = '72df5300299b9d3c2118ccf8b9e5f6f4',
          secret = 'SECRET'
        )
      ),
      portugal = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/anuncio/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.standvirtual.com/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$cars$portugal$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[3])===true)"),
        multipay = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/anunciar/')===true).filter(e=>e.name.includes('/')===false)",
        seller = ".filter(e=> _.contains(['standvirtual','www'],e.properties.$current_url.split('/')[2].split('.')[0])===false)",
        credentials = list(
          project_name = 'standvirtual.pt',
          token = 'b605c76ba537a8c09202d3b04c5acfa5',
          key = '536d7902bf81b48bab74ebbcb04f7a63',
          secret = 'SECRET'
        )
      ),
      romania = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/anunt/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.autovit.ro/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$cars$romania$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[3])===true)"),
        multipay = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/anunt-nou/')===true).filter(e=>e.name.includes('/')===false)",
        seller = ".filter(e=> _.contains(['autovit','www'],e.properties.$current_url.split('/')[2].split('.')[0])===false)",
        credentials = list(
          project_name = 'autovit.ro',
          token = 'adfe0536b9eb9cc099f7f35a4c7c9a02',
          key = 'c5d2a9f06e42506c66cdf8da2d406267',
          secret = 'SECRET'
        )
      )
    ),
    realestate = list(
      poland = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/oferta/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.otodom.pl/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$realestate$poland$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[4])===true)"),
        multipay = "",
        multipayb2c = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/nowe-ogloszenie/')===true).filter(e=>e.name.includes('/')===false)",
        seller = paste0("function getPath(item){if (item.properties.$current_url.includes('/inwestycje/')===true){return true}",
                        "else if(item.properties.$current_url.includes('/firmy/')===true){return true}",
                        "else if(item.properties.$current_url.includes('/shop/')===true){return true}",
                        "else if(_.contains(['otodom','www'],item.properties.$current_url.split('/')[2].split('.')[0])===false){return true}}",
                        ".filter(e=>getPath(e)===true)"),
        credentials = list(
          project_name = 'otodom.pl',
          token = '5da98ecd30a0b9103c5c42f2d2c5575b',
          key = '12877dfd1d62b1f6a69ed910e91d248a',
          secret = 'SECRET'
        )
      ),
      portugal = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/anuncio/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.imovirtual.com/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$realestate$portugal$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[4])===true)"),
        multipay = "",
        multipayb2c = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/novo-anuncio/')===true).filter(e=>e.name.includes('/')===false)",
        seller = ".filter(e=>e.properties.$current_url.includes('/agencia')===true)",
        credentials = list(
          project_name = 'imovirtual.pt',
          token = 'fbcae190c2396b3f725856d427c197d0',
          key = '494bb6c4faaccfa392e4dc1f72c97d54',
          secret = 'SECRET'
        )
      ),
      romania = list(
        account = "",
        adpage = ".filter(e=>e.properties.$current_url.includes('/oferta/')===true)",
        home = ".filter(e=>e.properties.$current_url==='https://www.storia.ro/')",
        listing = paste0(".filter(e=> _.contains(",paste0("['",paste(verticals_list$realestate$romania$db$categories$l1_cat$code, collapse = "','"),"']"),",e.properties.$current_url.split('/')[4])===true)"),
        multipay = "",
        multipayb2c = "",
        others = "",
        posting = ".filter(e=>e.properties.$current_url.includes('/anunt-nou/')===true).filter(e=>e.name.includes('/')===false)",
        seller = paste0("function getPath(item){if (item.properties.$current_url.includes('/investitii/')===true){return true}",
                        "else if(item.properties.$current_url.includes('/companii/')===true){return true}",
                        "else if(item.properties.$current_url.includes('/shop/')===true){return true}",
                        "else if(_.contains(['storia','www'],item.properties.$current_url.split('/')[2].split('.')[0])===false){return true}}",
                        ".filter(e=>getPath(e)===true)"),
        credentials = list(
          project_name = 'storia.ro',
          token = '6900af9e71311749fef8ca611dab940e',
          key = 'a3d12ec5d6428aa26aaaaa33ff2e7688',
          secret = 'SECRET'
        )
      )
    )
  ), # end list mixpanel
  hydra = list()
)
