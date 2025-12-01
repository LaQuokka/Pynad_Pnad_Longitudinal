# Este script mostra como pegar um schema unificado para o dataset completo.

library(arrow)
library(dplyr)

# Definir o caminho para a pasta
#caminho_da_pasta <- "C:/Users/Cinthya/Desktop/RA/research_assistence/painel_completo_filtrado_parquet"


caminho_da_pasta <- "C:/Users/digot/Dropbox/Public Finance/BPC/Paper/RA_cinthya/clean_data/painel_completo_filtrado_parquet_exc"



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

anos <- 2018
trimestres <- 1:4

trimestres_selecionados <- as.vector(outer(anos, trimestres, paste, sep = "_"))

# Use filter() para selecionar as linhas ANTES de trazê-las para a memória com collect()
dados_filtrados_df <- dataset %>%
  filter(yearq %in% trimestres_selecionados) %>%
  collect() # O collect() executa o filtro e traz o resultado para a memória

# Agora você pode visualizar o resultado
cat(sprintf("Foram encontradas %d linhas para os trimestres selecionados.\n", nrow(dados_filtrados_df)))
cat("Visualizando as 10 primeiras linhas do resultado:\n")
print(head(dados_filtrados_df, 10))