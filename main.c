#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h> // For uint32_t
#include <stdbool.h> // For true and false

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_ROWS 1400
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;
typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED_COMMAND } MetaCommandResult;
typedef enum { PREPARE_SUCCESS, PREPARE_STRING_TOO_LONG, PREPARE_NEGATIVE_ID, PREPARE_SYNTAX_ERROR, PREPARE_UNRECOGNIZED_STATEMENT } PrepareResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_DELETE, STATEMENT_SEARCH } StatementType;

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct Node {
    Row data;
    struct Node* left;
    struct Node* right;
} Node;

typedef struct {
    Node* root;
    uint32_t num_rows;
} Table;

typedef struct {
    Node* node;
    bool success;
} DeleteResult;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

ssize_t custom_getline(char **buffer, size_t *buffer_length, FILE *stream) {
    char *line = (char *)malloc(1024);
    if (!line) {
        return -1;
    }
    if (fgets(line, 1024, stream) == NULL) {
        free(line);
        return -1;
    }
    size_t len = strlen(line);
    if (len > 0 && line[len - 1] == '\n') {
        line[len - 1] = '\0';
        len--;
    }
    *buffer = line;
    *buffer_length = len;
    return len;
}

InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = custom_getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    input_buffer->input_length = bytes_read;
}

void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    strncpy(destination->username, source + USERNAME_OFFSET, USERNAME_SIZE);
    strncpy(destination->email, source + EMAIL_OFFSET, EMAIL_SIZE);
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

Node* create_node(Row* row) {
    Node* node = (Node*)malloc(sizeof(Node));
    node->data = *row;
    node->left = NULL;
    node->right = NULL;
    return node;
}

Node* find_minimum(Node* node) {
    while (node->left != NULL) {
        node = node->left;
    }
    return node;
}

Node* insert_node(Node* root, Row* row, uint32_t* num_rows) {
    if (*num_rows >= TABLE_MAX_ROWS) {
        printf("Error: La tabla esta llena.\n");
        return root;
    }
    if (root == NULL) {
        (*num_rows)++;
        return create_node(row);
    }
    if (row->id < root->data.id) {
        root->left = insert_node(root->left, row, num_rows);
    } else if (row->id > root->data.id) {
        root->right = insert_node(root->right, row, num_rows);
    } else {
        // El ID ya está en uso
        printf("El ID ya está ocupado.\n");
    }
    return root;
}
Node* search_node(Node* root, uint32_t id) {
    if (root == NULL || root->data.id == id) {
        return root;
    }
    if (id < root->data.id) {
        return search_node(root->left, id);
    } else {
        return search_node(root->right, id);
    }
}

ExecuteResult execute_insert(Statement* statement, Node** root, uint32_t* num_rows) {
    Row* row_to_insert = &(statement->row_to_insert);
    Node* new_root = insert_node(*root, row_to_insert, num_rows);
    *root = new_root;
    return EXECUTE_SUCCESS;
}

void in_order_traversal(Node* root) {
    if (root == NULL) {
        return;
    }
    in_order_traversal(root->left);
    print_row(&(root->data));
    in_order_traversal(root->right);
}
ExecuteResult execute_select(Statement* statement, Node* root) {
    in_order_traversal(root);
    return EXECUTE_SUCCESS;
}

DeleteResult delete_node(Node* root, uint32_t id) {
    DeleteResult result = {root, false};
    if (root == NULL) {
        return result;
    }
    if (id < root->data.id) {
        DeleteResult left_result = delete_node(root->left, id);
        root->left = left_result.node;
        result.success = left_result.success;
    } else if (id > root->data.id) {
        DeleteResult right_result = delete_node(root->right, id);
        root->right = right_result.node;
        result.success = right_result.success;
    } else {
        result.success = true;
        if (root->left == NULL) {
            Node* temp = root->right;
            free(root);
            result.node = temp;
            return result;
        } else if (root->right == NULL) {
            Node* temp = root->left;
            free(root);
            result.node = temp;
            return result;
        }
        Node* temp = find_minimum(root->right);
        root->data = temp->data;
        DeleteResult right_result = delete_node(root->right, temp->data.id);
        root->right = right_result.node;
    }
    result.node = root;
    return result;
}

ExecuteResult execute_delete(Statement* statement, Node** root, uint32_t* num_rows) {
    uint32_t id_to_delete = statement->row_to_insert.id;
    DeleteResult result = delete_node(*root, id_to_delete);
    *root = result.node;
    if (!result.success) {
        printf("ID no encontrado.\n");
    }
    (*num_rows)--;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_search(uint32_t id, Node* root) {
    Node* node = search_node(root, id);
    if (node == NULL) {
        printf("ID no encontrado.\n");
    } else {
        print_row(&(node->data));
        return EXECUTE_SUCCESS;
    }
}

ExecuteResult execute_statement(Statement* statement, Node** root, uint32_t* num_rows) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, root, num_rows);
        case (STATEMENT_SELECT):
            return execute_select(statement, *root);
        case (STATEMENT_DELETE):
            return execute_delete(statement, root, num_rows);
        case (STATEMENT_SEARCH):
            return execute_search(statement->row_to_insert.id, *root);
    }
}
void save_tree_to_file(Node* node, FILE* file) {
    if (node == NULL) {
        return;
    }
    // Serializar los datos de la fila y escribir en el archivo
    fprintf(file, "%d,%s,%s\n", node->data.id, node->data.username, node->data.email);
    save_tree_to_file(node->left, file);
    save_tree_to_file(node->right, file);
}
void db_close(Node* root) {
    FILE* file = fopen("C:/Users/Roberto/CLionProjects/BaseDeDatosSimple/mi_base_de_datos.db", "w");
    if (file == NULL) {
        printf("Unable to open file for writing.\n");
        exit(EXIT_FAILURE);
    }
    save_tree_to_file(root, file);
    fclose(file);
}
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Node* root) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        printf("Saving database to disk...\n");
        db_close(root);
        printf("Database saved. Exiting.\n");
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

PrepareResult prepare_delete(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_DELETE;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    if (id_string == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    statement->row_to_insert.id = id;
    return PREPARE_SUCCESS;
}

PrepareResult prepare_search(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_SEARCH;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    if (id_string == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    statement->row_to_insert.id = id;
    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
        return prepare_delete(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "search", 6) == 0) {
        return prepare_search(input_buffer, statement);
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}



Node* load_tree_from_file(FILE* file, uint32_t* row_count) {
    Node* root = NULL;
    char line[1024];
    *row_count = 0;
    while (fgets(line, sizeof(line), file)) {
        Row row;
        sscanf(line, "%d,%31[^,],%254[^\n]", &row.id, row.username, row.email);
        root = insert_node(root, &row, row_count);
    }
    return root;
}



Table* db_open() {
    FILE* file = fopen("C:/Users/Roberto/CLionProjects/BaseDeDatosSimple/mi_base_de_datos.db", "r");
    Table* table = (Table*)malloc(sizeof(Table));
    table->root = NULL;
    table->num_rows = 0;
    if (file == NULL) {
        printf("No se encontró la base de datos. Se creará una nueva.\n");
        return table;
    }
    uint32_t row_count = 0;
    table->root = load_tree_from_file(file, &row_count);
    table->num_rows = row_count;
    fclose(file);
    printf("Base de datos abierta con %d filas.\n", row_count);
    return table;
}

int main(int argc, char* argv[]) {
    Table* table = db_open();
    InputBuffer* input_buffer = new_input_buffer();

    // Ruta fija del archivo de pruebas
    const char* test_file_path = "C:/Users/Roberto/CLionProjects/BaseDeDatosSimple/test_cases1.txt";
    FILE* input_stream = fopen(test_file_path, "r");

    if (!input_stream) {
        fprintf(stderr, "Error: No se pudo abrir el archivo '%s'.\n", test_file_path);
        exit(EXIT_FAILURE);
    }

    while (true) {
        // Leer entrada desde el archivo
        ssize_t bytes_read = custom_getline(&(input_buffer->buffer), &(input_buffer->buffer_length), input_stream);
        if (bytes_read <= 0) { // Fin del archivo
            break;
        }
        input_buffer->input_length = bytes_read;

        // Imprimir la acción actual
        printf("Ejecutando: %s\n", input_buffer->buffer);

        // Procesar comandos meta
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table->root)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Comando no reconocido: '%s'.\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
            continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
            continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
            continue;
        }

        switch (execute_statement(&statement, &(table->root), &(table->num_rows))) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
            break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
            break;
        }
    }

    fclose(input_stream);
    close_input_buffer(input_buffer);
    db_close(table->root);
    free(table);
    return 0;
}