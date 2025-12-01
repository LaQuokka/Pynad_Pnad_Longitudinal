# Este arquivo constrói um dicionário com as descrições de todas as chaves disponíveis
# no painel completo do Pynad (excluindo as variáveis suplementares).


install.packages("writexl")  
library(stringr)
library(dplyr)
library(readr)
library(arrow)
library(dplyr)
library(writexl)

# Definir o caminho para a pasta
caminho_da_pasta <- "C:/Users/Cinthya/Desktop/RA/research_assistence/painel_completo_filtrado_parquet"

lista_de_arquivos <- list.files(caminho_da_pasta, pattern = "\\.parquet$", full.names = TRUE)

# Extrair o esquema de cada arquivo
lista_de_esquemas <- lapply(lista_de_arquivos, function(arquivo) {
  return(open_dataset(arquivo)$schema)
})

# Unificar os esquemas usando o método do.call
esquema_unificado <- do.call(unify_schemas, lista_de_esquemas)

# Abrir o dataset usando o esquema COMPLETO
dataset <- open_dataset(
  sources = caminho_da_pasta, 
  schema = esquema_unificado,
  format = "parquet"
)

cat(sprintf("A base de dados (com esquema unificado) possui %d colunas.\n", ncol(dataset)))

primeiras_10_linhas <- dataset %>%
  head(10) %>%
  collect()

nomes_colunas <- colnames(primeiras_10_linhas)

pasta_new <- "C:/Users/Cinthya/Desktop/RA/research_assistence/create_dictionary/arquivos"
arquivos_new <- list.files(pasta_new, pattern = "\\.txt$", full.names = TRUE)
arquivos_new <- sort(arquivos_new, decreasing = TRUE)  # ordem alfabética decrescente

for (arquivo in arquivos_new) {
  cat(sprintf("Processando arquivo: %s\n", arquivo))
  
  linhas_new <- read_lines(arquivo)
  
  dicionario_ibge_new <- linhas_new %>%
    str_subset("/\\*.*\\*/") %>%
    tibble(linha = .) %>%
    mutate(
      chave_original = str_extract(linha, "\\b[A-Za-z0-9_]+\\b(?=\\s+[\\$0-9])"),
      descricao = str_extract(linha, "(?<=/\\*).*(?=\\*/)") %>% str_trim()
    ) %>%
    filter(!is.na(chave_original)) %>%
    mutate(chave = tolower(chave_original)) %>%
    select(chave, descricao)
  
  dicionario_final <- dicionario_final %>%
    left_join(dicionario_ibge_new, by = "chave", suffix = c("", "_new")) %>%
    mutate(
      descricao = if_else(is.na(descricao) | descricao == "", descricao_new, descricao),
      fonte = if_else(
        (!is.na(descricao_new) & (is.na(descricao) | descricao == descricao_new)),
        arquivo,
        fonte
      )
    ) %>%
    select(chave, descricao, fonte)
}

sum(is.na(dicionario_final$descricao))

dicionario_final <- dicionario_final %>%
  mutate(
    descricao = case_when(
      chave == "pid" ~ "código do painel no formato AAAAT",
      chave == "pidgrp" ~ "número sequencial do grupo doméstico no domicílio [1,5]",
      chave == "pidgrpent" ~ "código das visitas com entrevistas do grupo doméstico [1,31]",
      chave == "pidind" ~ "número sequencial do indivíduo no grupo doméstico",
      chave == "pidindent" ~ "código das visitas com entrevistas do indivíduo [1,31]",
      chave == "pidcla" ~ "código da classe de identificação do indivíduo [1,7]",
      chave == "piddnd" ~ "dia de nascimento imputado para data ignorada",
      chave == "piddnm" ~ "mês de nascimento inputado para data ignorada",
      chave == "piddna" ~ "ano de nascimento imputado para data ignorada",
      TRUE ~ descricao
    ),
    fonte = case_when(
      chave %in% c("pid","pidgrp","pidgrpent","pidind","pidindent","pidcla","piddnd","piddnm","piddna") ~ "pynad",
      TRUE ~ fonte
    )
  )
sum(is.na(dicionario_final$descricao))

dicionario_final <- dicionario_final %>%
  mutate(
    descricao = case_when(
      chave == "chave_dom" ~ "chave única de domicílio",
      chave == "chave_ind" ~ "chave única de indivíduo",
      chave == "interviews" ~ "entrevistas que o indivíduo fez",
      chave == "yearq" ~ "ano e trimestre da entrevista no formato YYYYT",
      chave == "entrevista" ~ "número da entrevista feita ao indivíduo",
      chave == "familia_elegivel" ~ "TRUE se o domicilio contiver um idoso como membro mais velho do domicilio na entrevista 1 e FALSE caso contrario",
      chave == "idoso_entrevista1" ~ "TRUE se este individuo era idoso (pelo menos 65 anos) na entrevista 1 e FALSE caso contrario",
      chave == "membro_mais_velho_entrevista1" ~ "TRUE se este individuo foi o individuo mais velho do domicilio na entrevista 1 e FALSE caso contrario",
      chave == "idade_membro_mais_velho_entrevista1" ~ "idade que o membro mais velho do domicílio tinha na entrevista 1",
      chave == "panel_file" ~ "painel do pynad que é a fonte dessa observação",
      TRUE ~ descricao
    ),
    fonte = case_when(
      chave %in% c(
        "chave_dom","chave_ind","interviews","yearq","entrevista",
        "familia_elegivel","idoso_entrevista1","membro_mais_velho_entrevista1",
        "idade_membro_mais_velho_entrevista1","panel_file"
      ) ~ "manual",
      TRUE ~ fonte
    )
  )

sum(is.na(dicionario_final$descricao))

dicionario_final <- dicionario_final %>%
  filter(descricao != "remover")


# Exportar o dicionário final para Excel
write_xlsx(dicionario_final, "C:/Users/Cinthya/Desktop/RA/research_assistence/dicionario_final.xlsx")
