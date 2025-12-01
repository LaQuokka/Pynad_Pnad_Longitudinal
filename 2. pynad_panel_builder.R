# Este código lê os arquivos de painel, os converte para arquivos Parquet temporários
# e, em seguida, unifica todos em um único arquivo Parquet final.

# --- CONFIGURAÇÃO ---
pasta_zips <- "C:/Users/Cinthya/Desktop/RA/research_assistence/pnadc/paineis/csv"

pasta_temporaria <- file.path(tempdir(), "paineis_parquet_temp") # Cria um diretório temporário para os arquivos Parquet individuais

caminho_saida_parquet <- "C:/Users/Cinthya/Desktop/RA/research_assistence/painel_completo_filtrado_parquet"

# install.packages("data.table")
# install.packages("tidyverse")
# install.packages("combinat")
# install.packages("arrow")

### 1. Importação de pacotes ###
library(data.table)
library(dplyr)
library(tidyr)
library(stringr)
library(combinat)
library(arrow)

#-----------------------------------------------------------------------------#
# (Funções: read_file, create_interviews_list, create_columns_treatment, #
# interviews_dictionary, create_panel, reorder_columns, build_panel,         #
# e check_panel permanecem exatamente as mesmas que no seu script original)    #
#-----------------------------------------------------------------------------#
#' Lê um painel a partir do caminho do seu zip e da lista de colunas a serem lidas.
#'
#' Se cols2load for uma lista vazia, todas as colunas serão lidas.
#' Se for solicitada a leitura de alguma coluna que não existe no painel, essa coluna será ignorada.
#'
#' @param caminho_zip Caminho para o arquivo .zip.
#' @param rename_dict Dicionário (lista nomeada em R) para referência de nomes.
#' @param cols2load Vetor de colunas a serem carregadas. Default é NULL (carregar todas).
#' @return Um data.frame (na verdade, um tibble) com os dados lidos.
read_file <- function(caminho_zip, rename_dict, cols2load = NULL) {
  
  # Extrai o nome do arquivo CSV de dentro do ZIP
  csv_file_name <- unzip(caminho_zip, list = TRUE)$Name[1]
  
  # Monta o comando de leitura para o data.table::fread
  zip_command <- paste("unzip -p", shQuote(caminho_zip), csv_file_name)
  
  file_short_name <- str_sub(caminho_zip, -9, -5)
  cat(paste0("Arquivo que será lido: ", file_short_name, "...\n"))
  
  # Lê apenas o cabeçalho para obter as colunas disponíveis
  available_cols <- names(fread(cmd = zip_command, nrows = 0))
  
  cols <- c()
  if (!is.null(cols2load)) {
    cols <- intersect(cols2load, available_cols)
    not_found <- setdiff(cols2load, available_cols)
    
    if (length(not_found) > 0) {
      # Dicionário das colunas não encontradas
      not_found_dict_keys <- not_found
      cat("As seguintes colunas não foram encontradas no painel: ", toString(not_found_dict_keys), "\n")
    }
  } else {
    cols <- available_cols
  }
  
  # Lê o arquivo de fato, selecionando as colunas
  df <- fread(cmd = zip_command, select = cols) %>% as_tibble()
  
  # Monta o dicionário de códigos -> descrições das colunas carregadas
  col_dict_keys <- names(df)
  #cat("Dicionário de colunas lidas: ", toString(col_dict_keys), "\n")
  cat("Colunas de ", file_short_name, "lidas.\n")
  return(df)
}

#-----------------------------------------------------------------------------#

#' Criando um dicionário onde a chave é o pidindent e o valor é a lista com as entrevistas das quais o indivíduo participou
#'
#' Lógica:
#' O indivíduo pode participar de até 5 entrevistas. Ele deve participar de pelo menos uma. Ele inicia com um score = 0.
#' Se ele participa da primeira entrevista, é somado 2^0 a seu score. Se ele participa da n-ésima entrevista, é somado 2^(n-1) ao seu score.
#' Assim, cria-se um score que representa um código para a combinação de entrevistas das quais este indivíduo participou.
#'
#' @return Uma lista nomeada onde os nomes são os scores e os valores são os vetores de entrevistas.
create_interviews_list <- function() {
  n_entrevistas <- 5
  score_dict <- list()
  
  # Itera sobre todas as combinações possíveis de pelo menos uma entrevista
  for (r in 1:n_entrevistas) {
    # `combn` gera combinações. `simplify = FALSE` retorna uma lista de vetores.
    combos <- combn(1:n_entrevistas, r, simplify = FALSE)
    
    for (combo in combos) {
      score <- sum(2^(combo - 1))
      score_dict[[as.character(score)]] <- combo
    }
  }
  
  return(score_dict)
}

#-----------------------------------------------------------------------------#

#' Cria colunas chave_ind, chave_dom, idoso_1, membro_mais_velho_1, familia_elegivel e domicilio_BPC.
#'
#' Gera erro se não existir nenhuma coluna BPC para criar domicilio_BPC.
#'
#' @param df O data.frame de entrada.
#' @return O data.frame com as novas colunas.
create_columns_treatment <- function(df) {
  
  df <- df %>%
    # Cria chave_ind, chave_dom e idoso_1
    mutate(
      chave_ind = paste0(pid, upa, v1008, pidgrp, pidind),
      chave_dom = paste0(upa, v1008, v1014),
      idoso_entrevista1 = (v2009_1 >= 50) & (v2009_1 <= 70) # v2009: idade
    ) %>%
    # Cria membro_mais_velho_entrevista1 com base em idade_1
    group_by(chave_dom) %>%
    mutate(
      membro_mais_velho_entrevista1 = (v2009_1 == max(v2009_1, na.rm = TRUE)) # v2009: idade
    ) %>%
    # Calcula um booleano por família se existe alguém que seja idoso e membro mais velho
    mutate(
      familia_elegivel = any(idoso_entrevista1 & membro_mais_velho_entrevista1, na.rm = TRUE)
    ) %>%
    ungroup()
  
  # --- NOVO BLOCO: Loop para criar as colunas de idade do membro mais velho ---
  # Este loop traduz a lógica do seu código Python.
  for (i in 1:5) {
    
    # Nomes dinâmicos para a coluna de idade de entrada e a coluna de saída
    col_idade <- paste0("v2009_", i)
    col_saida <- paste0("idade_membro_mais_velho_entrevista1_", i)
    
    # Verifica se a coluna de idade daquela entrevista existe no data.frame antes de continuar
    if (!col_idade %in% names(df)) {
      next # Pula para a próxima iteração do loop se a coluna não existir
    }
    
    df <- df %>%
      group_by(chave_dom) %>%
      mutate(
        # Usamos `:=` para criar uma coluna com nome dinâmico (o valor de `col_saida`)
        # A lógica é:
        # 1. if_else: Se for o membro mais velho, use a idade da entrevista `i`, senão use NA.
        # 2. max(...): Pegue o único valor de idade não-NA no grupo e replique para todos.
        "{col_saida}" := max(
          if_else(membro_mais_velho_entrevista1, .data[[col_idade]], NA_real_),
          na.rm = TRUE
        )
      ) %>%
      ungroup()
  }
  
  return(df)
}

#-----------------------------------------------------------------------------#

#' Função auxiliar para construir a coluna year_q.
#'
#' Recebe como input o ano e trimestre do arquivo. Sabendo que esse ano/tri representa a 5ª entrevista,
#' define o ano/tri das demais entrevistas.
#'
#' @param ano Ano da 5ª entrevista.
#' @param tri Trimestre da 5ª entrevista.
#' @return Um vetor nomeado com o mapeamento da entrevista para o ano/trimestre.
interviews_dictionary <- function(ano, tri) {
  ano_num <- as.numeric(ano)
  d <- c()
  
  switch(tri,
         '1' = {
           d <- c('1' = paste0(ano_num - 1, "_1"), '2' = paste0(ano_num - 1, "_2"), '3' = paste0(ano_num - 1, "_3"), '4' = paste0(ano_num - 1, "_4"), '5' = paste0(ano_num, "_1"))
         },
         '2' = {
           d <- c('1' = paste0(ano_num - 1, "_2"), '2' = paste0(ano_num - 1, "_3"), '3' = paste0(ano_num - 1, "_4"), '4' = paste0(ano_num, "_1"), '5' = paste0(ano_num, "_2"))
         },
         '3' = {
           d <- c('1' = paste0(ano_num - 1, "_3"), '2' = paste0(ano_num - 1, "_4"), '3' = paste0(ano_num, "_1"), '4' = paste0(ano_num, "_2"), '5' = paste0(ano_num, "_3"))
         },
         '4' = {
           d <- c('1' = paste0(ano_num - 1, "_4"), '2' = paste0(ano_num, "_1"), '3' = paste0(ano_num, "_2"), '4' = paste0(ano_num, "_3"), '5' = paste0(ano_num, "_4"))
         }
  )
  
  return(d)
}

#-----------------------------------------------------------------------------#

#' Transforma o data.frame de formato wide para long.
#'
#' @param df_to_panel O data.frame de entrada (wide).
#' @param file O nome do arquivo original, usado para extrair ano e trimestre.
#' @return O data.frame em formato de painel (long).
create_panel <- function(df_to_panel, file) {
  
  all_cols <- names(df_to_panel)
  cols_sufixo <- all_cols[str_ends(all_cols, "_[1-5]")]
  id_vars <- setdiff(all_cols, cols_sufixo)
  
  # Extrai os prefixos (stubnames)
  stubnames <- unique(str_remove(cols_sufixo, "_[1-5]$"))
  
  # Equivalente ao pd.wide_to_long é o tidyr::pivot_longer
  panel <- df_to_panel %>%
    pivot_longer(
      cols = all_of(cols_sufixo),
      names_to = c(".value", "entrevista"),
      names_sep = "_(?=[1-5]$)", # Separa no último _ seguido por um número de 1 a 5
      names_transform = list(entrevista = as.integer)
    ) %>%
    arrange(chave_dom, chave_ind, entrevista)
  
  score_dict <- create_interviews_list()
  
  year <- str_sub(file, -9, -6)
  tri <- str_sub(file, -5, -5)
  interview_date_map <- interviews_dictionary(year, tri)
  
  # Mapeia os valores usando as listas/vetores nomeados
  panel <- panel %>%
    mutate(
      interviews = score_dict[as.character(pidindent)],
      yearq = interview_date_map[as.character(entrevista)]
    )
  
  return(panel)
}

#-----------------------------------------------------------------------------#

#' Reordena as colunas do DataFrame, colocando primeiro as colunas desejadas
#' que existem no df e depois as demais colunas.
#'
#' @param df DataFrame a ser reordenado.
#' @param cols_prioritarias Vetor de colunas que devem aparecer primeiro.
#' @return DataFrame com colunas reordenadas.
reorder_columns <- function(df, cols_prioritarias) {
  # Mantém apenas as colunas prioritárias que existem no DataFrame
  cols_existentes <- intersect(cols_prioritarias, names(df))
  
  # `everything()` do dplyr seleciona todas as outras colunas
  df <- df %>%
    select(all_of(cols_existentes), everything())
  
  return(df)
}

#-----------------------------------------------------------------------------#

#' Constrói o painel a partir de um arquivo.
#'
#' @param file Caminho para o arquivo .zip.
#' @param rename_dict Dicionário de renomeação.
#' @param columns_to_read Vetor de colunas a serem lidas.
#' @return Um data.frame (tibble) finalizado.
build_panel <- function(file, rename_dict, columns_to_read = NULL) {
  if (!is.null(columns_to_read)) {
    df <- read_file(file, rename_dict, columns_to_read)
  } else {
    df <- read_file(file, rename_dict)
  }
  
  df <- create_columns_treatment(df)
  df <- df %>% filter(familia_elegivel == TRUE)
  df <- create_panel(df, file)
  
  cols_prioritarias <- c('chave_dom','chave_ind','interviews','yearq','entrevista','sexo', 'idade','idoso','familia_elegivel','membro_mais_velho','domicilio_BPC',
                         'ocupacao', 'cod_ocupacao', 'escolaridade')
  df <- reorder_columns(df, cols_prioritarias)
  
  df <- df %>%
    arrange(chave_dom, chave_ind, entrevista) %>%
    mutate(panel_file = str_sub(file, -9, -5))
  
  return(df)
}

### 2. Dicionários para loading ###

rename_dict_2015 <- list(
  'v1016_1'='n_entrevista_1', 'v1016_2'='n_entrevista_2', 'v1016_3'='n_entrevista_3', 'v1016_4'='n_entrevista_4', 'v1016_5'='n_entrevista_5',
  'v2007_1'='sexo_1', 'v2007_2'='sexo_2', 'v2007_3'='sexo_3', 'v2007_4'='sexo_4', 'v2007_5'='sexo_5',
  'v2009_1'='idade_1', 'v2009_2'='idade_2', 'v2009_3'='idade_3', 'v2009_4'='idade_4', 'v2009_5'='idade_5',
  'v3009a_1'='escolaridade_1', 'v3009a_2'='escolaridade_2', 'v3009a_3'='escolaridade_3', 'v3009a_4'='escolaridade_4', 'v3009a_5'='escolaridade_5',
  'vd4002_1'='ocupacao_1', 'vd4002_2'='ocupacao_2', 'vd4002_3'='ocupacao_3', 'vd4002_4'='ocupacao_4', 'vd4002_5'='ocupacao_5',
  'v4010_1'='cod_ocupacao_1', 'v4010_2'='cod_ocupacao_2', 'v4010_3'='cod_ocupacao_3', 'v4010_4'='cod_ocupacao_4', 'v4010_5'='cod_ocupacao_5',
  'vd4020_1'='renda_trabalho_1', 'vd4020_2'='renda_trabalho_2', 'vd4020_3'='renda_trabalho_3', 'vd4020_4'='renda_trabalho_4', 'vd4020_5'='renda_trabalho_5',
  'v5001a_1'='recebe_BPC_v_1', 'v5001a_2'='recebe_BPC_v_2', 'v5001a_3'='recebe_BPC_v_3', 'v5001a_4'='recebe_BPC_v_4', 'v5001a_5'='recebe_BPC_v_5',
  'v5001a2_1'='renda_BPC_v_1', 'v5001a2_2'='renda_BPC_v_2', 'v5001a2_3'='renda_BPC_v_3', 'v5001a2_4'='renda_BPC_v_4', 'v5001a2_5'='renda_BPC_v_5',
  'v5004a_1'='recebe_aposentadoria_v_1', 'v5004a_2'='recebe_aposentadoria_v_2', 'v5004a_3'='recebe_aposentadoria_v_3', 'v5004a_4'='recebe_aposentadoria_v_4', 'v5004a_5'='recebe_aposentadoria_v_5',
  'v5004a2_1'='renda_aposentadoria_v_1', 'v5004a2_2'='renda_aposentadoria_v_2', 'v5004a2_3'='renda_aposentadoria_v_3', 'v5004a2_4'='renda_aposentadoria_v_4', 'v5004a2_5'='renda_aposentadoria_v_5',
  'v5002a_1'='recebe_bolsa_familia_v_1', 'v5002a_2'='recebe_bolsa_familia_v_2', 'v5002a_3'='recebe_bolsa_familia_v_3', 'v5002a_4'='recebe_bolsa_familia_v_4', 'v5002a_5'='recebe_bolsa_familia_v_5',
  'v5002a2_1'='renda_bolsa_familia_v_1', 'v5002a2_2'='renda_bolsa_familia_v_2', 'v5002a2_3'='renda_bolsa_familia_v_3', 'v5002a2_4'='renda_bolsa_familia_v_4', 'v5002a2_5'='renda_bolsa_familia_v_5',
  "pid"="pid", "upa"="upa", "v1008"="v1008", "pidgrp"="pidgrp", "pidind"="pidind", "v1014"="v1014", "pidindent"="pidindent",
  "trimestre_1"="trimestre_1", "trimestre_2"="trimestre_2", "trimestre_3"="trimestre_3", "trimestre_4"="trimestre_4", "trimestre_5"="trimestre_5",
  "ano_1"="ano_1", "ano_2"="ano_2", "ano_3"="ano_3", "ano_4"="ano_4", "ano_5"="ano_5"
)

rename_dict_12_14 <- list(
  'v1016_1'='n_entrevista_1', 'v1016_2'='n_entrevista_2', 'v1016_3'='n_entrevista_3', 'v1016_4'='n_entrevista_4', 'v1016_5'='n_entrevista_5',
  'v2007_1'='sexo_1', 'v2007_2'='sexo_2', 'v2007_3'='sexo_3', 'v2007_4'='sexo_4', 'v2007_5'='sexo_5',
  'v2009_1'='idade_1', 'v2009_2'='idade_2', 'v2009_3'='idade_3', 'v2009_4'='idade_4', 'v2009_5'='idade_5',
  'v3009_1'='escolaridade_1', 'v3009_2'='escolaridade_2', 'v3009_3'='escolaridade_3', 'v3009_4'='escolaridade_4', 'v3009_5'='escolaridade_5',
  'vd4002_1'='ocupacao_1', 'vd4002_2'='ocupacao_2', 'vd4002_3'='ocupacao_3', 'vd4002_4'='ocupacao_4', 'vd4002_5'='ocupacao_5',
  'v4010_1'='cod_ocupacao_1', 'v4010_2'='cod_ocupacao_2', 'v4010_3'='cod_ocupacao_3', 'v4010_4'='cod_ocupacao_4', 'v4010_5'='cod_ocupacao_5',
  'vd4020_1'='renda_trabalho_1', 'vd4020_2'='renda_trabalho_2', 'vd4020_3'='renda_trabalho_3', 'vd4020_4'='renda_trabalho_4', 'vd4020_5'='renda_trabalho_5',
  'v50091_1'='recebe_BPC_v_1', 'v50091_2'='recebe_BPC_v_2', 'v50091_3'='recebe_BPC_v_3', 'v50091_4'='recebe_BPC_v_4', 'v50091_5'='recebe_BPC_v_5',
  'v500911_1'='renda_BPC_v_1', 'v500911_2'='renda_BPC_v_2', 'v500911_3'='renda_BPC_v_3', 'v500911_4'='renda_BPC_v_4', 'v500911_5'='renda_BPC_v_5',
  'v50011_1'='recebe_aposentadoria_v_1', 'v50011_2'='recebe_aposentadoria_v_2', 'v50011_3'='recebe_aposentadoria_v_3', 'v50011_4'='recebe_aposentadoria_v_4', 'v50011_5'='recebe_aposentadoria_v_5',
  'v500111_1'='renda_aposentadoria_v_1', 'v500111_2'='renda_aposentadoria_v_2', 'v500111_3'='renda_aposentadoria_v_3', 'v500111_4'='renda_aposentadoria_v_4', 'v500111_5'='renda_aposentadoria_v_5',
  'v50101_1'='recebe_bolsa_familia_v_1', 'v50101_2'='recebe_bolsa_familia_v_2', 'v50101_3'='recebe_bolsa_familia_v_3', 'v50101_4'='recebe_bolsa_familia_v_4', 'v50101_5'='recebe_bolsa_familia_v_5',
  'v501011_1'='renda_bolsa_familia_v_1', 'v501011_2'='renda_bolsa_familia_v_2', 'v501011_3'='renda_bolsa_familia_v_3', 'v501011_4'='renda_bolsa_familia_v_4', 'v501011_5'='renda_bolsa_familia_v_5',
  "pid"="pid", "upa"="upa", "v1008"="v1008", "pidgrp"="pidgrp", "pidind"="pidind", "v1014"="v1014", "pidindent"="pidindent",
  "trimestre_1"="trimestre_1", "trimestre_2"="trimestre_2", "trimestre_3"="trimestre_3", "trimestre_4"="trimestre_4", "trimestre_5"="trimestre_5",
  "ano_1"="ano_1", "ano_2"="ano_2", "ano_3"="ano_3", "ano_4"="ano_4", "ano_5"="ano_5"
)


### 3. Construção do Painel em Duas Etapas (Robusto e com Pouca Memória) ###


# --- LÓGICA DE LIMPEZA ---
# Remove a pasta de saída final se ela existir de uma execução anterior
#if (dir.exists(caminho_saida_parquet)) {
#  cat("Pasta de saída final já existe. Removendo...\n")
#  unlink(caminho_saida_parquet, recursive = TRUE)
#}
cat("Criando nova pasta de saída para o dataset Parquet.\n")
dir.create(caminho_saida_parquet)


# --- Processar e Salvar Arquivos Parquet Individuais ---
arquivos <- list.files(pasta_zips, pattern = "\\.zip$", full.names = TRUE)
arquivos <- arquivos[32:44]
cat(paste("\nIniciando processamento de", length(arquivos), "arquivos...\n\n"))

for (file in arquivos) {
  # ... Lógica de seleção de dicionário ...
  ano_arquivo <- str_sub(file, -9, -6)
  ano_trim_arquivo <- str_sub(file, -9, -5)
  if (ano_arquivo %in% c('2012', '2013', '2014') || ano_trim_arquivo == '20151') {
    rename_dict <- rename_dict_12_14
  } else {
    rename_dict <- rename_dict_2015
  }
  columns_to_read <- names(rename_dict)
  
  # Processa o painel individual
  df_processado <- build_panel(file, rename_dict)
  
  # --- BLOCO DE VERIFICAÇÃO ADICIONADO ---
  cat("--- Verificando a presença de variáveis-chave no painel processado ---\n")
  vars_para_checar <- c("v5001a", "v5002a", "v5004a", "v50091", "v50011", "v50101", "s01010")
  
  # Pega as colunas do dataframe *depois* do processamento e do pivot
  colunas_processadas <- colnames(df_processado)
  
  variaveis_encontradas <- intersect(vars_para_checar, colunas_processadas)
  
  if (length(variaveis_encontradas) > 0) {
    cat(">>> Variáveis encontradas:", paste(variaveis_encontradas, collapse = ", "), "\n")
  } else {
    cat(">>> Nenhuma das variáveis-chave foi encontrada neste painel.\n")
  }
  # --- FIM DO BLOCO DE VERIFICAÇÃO ---
  
  # Converte explicitamente para uma Tabela Arrow para evitar erros de metadados
  tabela_arrow <- arrow_table(df_processado)
  
  # Define o caminho para o arquivo individual DENTRO da pasta de saída
  nome_painel <- sub(".zip", "", basename(file))
  caminho_arquivo_individual <- file.path(caminho_saida_parquet, paste0(nome_painel, ".parquet"))
  
  # Salva o arquivo Parquet individual
  write_parquet(tabela_arrow, sink = caminho_arquivo_individual)
  
  cat(paste("Painel", nome_painel, "processado e salvo com sucesso.\n"))
  
  # Limpeza de memória
  rm(df_processado, tabela_arrow)
  gc()
  cat("-----------------------------------------------------------\n")
}

cat("\nProcesso finalizado com sucesso!\n")
cat("Seu dataset Parquet particionado está pronto para ser usado na pasta:\n")
cat(caminho_saida_parquet, "\n")