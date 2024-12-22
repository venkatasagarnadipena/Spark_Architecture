# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

df=spark.read.format("parquet").load("/mnt/adls/target_tables/Dim/customers_table/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Understanding Job, Stage, and Task Creation in Spark Architecture
# MAGIC
# MAGIC ### Example Question:
# MAGIC We need to provide the count of customers whose phone number ends with '7' from the given DataFrame.
# MAGIC
# MAGIC ### Breakdown of Spark Execution Flow
# MAGIC
# MAGIC ### Explanation of the Flow:
# MAGIC
# MAGIC 1. **User Query**:
# MAGIC    - The user writes a query to filter customers whose phone number ends with '7' (e.g., df.filter(df.Phone.endswith("7")).count()).
# MAGIC
# MAGIC
# MAGIC 2. **Driver Program**:
# MAGIC    - The Driver Program parses the user query and constructs a Directed Acyclic Graph (DAG) that represents the stages of computation.
# MAGIC    - The Driver passes this logical plan to the Catalyst Optimizer for optimization.
# MAGIC    
# MAGIC    1. ***Catalyst Optimizer***:
# MAGIC       - The Catalyst Optimizer applies rule-based transformations to optimize the logical plan.
# MAGIC       - It generates one or more physical plans based on the optimization and may apply cost-based optimization to choose the most efficient plan.
# MAGIC    
# MAGIC    - The driver program will create stages to apply the filter and perform the action to count the results.
# MAGIC    - The driver program reaches out to the *Cluster Manager* for resource management.
# MAGIC
# MAGIC 3. **Cluster Manager**:
# MAGIC    - The cluster manager allocates resources (memory, CPU) and divides the data into partitions.
# MAGIC    - The manager sends each partition to different worker nodes for parallel execution.
# MAGIC
# MAGIC 4. **Worker Nodes (Executors)**:
# MAGIC    - The worker nodes execute the task of filtering customers whose phone numbers end with '7'.
# MAGIC    - Multiple worker nodes will process the partitions of the data in parallel.
# MAGIC
# MAGIC 5. **Driver Program**:
# MAGIC    - After task execution, the driver collects the filtered results from the worker nodes.
# MAGIC    - The driver aggregates the results and returns the count of customers whose phone numbers end with '7' to the user.
# MAGIC
# MAGIC ---
# MAGIC ## Flow Diagram
# MAGIC
# MAGIC ```plaintext
# MAGIC +-------------------+
# MAGIC |   User Query      |         (Write query in notebook)
# MAGIC |                   |         (Example: df.filter(col("Phone").cast(StringType()).like("%7")).count())
# MAGIC +-------------------+ 
# MAGIC          |
# MAGIC          v
# MAGIC +-------------------+     
# MAGIC |   Driver Program  |         (Creates DAG, plans execution)
# MAGIC |                   |         (Filter rows, create actions)
# MAGIC +-------------------+
# MAGIC          |
# MAGIC          v
# MAGIC +-------------------+
# MAGIC | Catalyst Optimizer|         (Optimizes the query plan)
# MAGIC |                   |         (Logical Plan -> Optimized Logical Plan -> Physical Plan)
# MAGIC +-------------------+
# MAGIC          |
# MAGIC          v
# MAGIC +-------------------+     
# MAGIC |  Cluster Manager  |         (Allocates resources, partitions data)
# MAGIC +-------------------+  
# MAGIC          |
# MAGIC          v
# MAGIC +-------------------+        +-------------------+
# MAGIC | Worker Node 1     |<------>| Worker Node 2     |      (Parallel execution of tasks)
# MAGIC | (Executor)        |        | (Executor)        |
# MAGIC +-------------------+        +-------------------+ 
# MAGIC          |                        |
# MAGIC          v                        v
# MAGIC +-------------------+        +-------------------+
# MAGIC | Task Execution    |        | Task Execution    |      (Filtering phone numbers ending with 7)
# MAGIC | (on Data Partitions)|      | (on Data Partitions)|
# MAGIC +-------------------+        +-------------------+
# MAGIC          |
# MAGIC          v
# MAGIC +-------------------+
# MAGIC | Driver Program    |         (Collect results and send them to user)
# MAGIC | (Count result)    |         (Final count of customers)
# MAGIC +-------------------+
# MAGIC

# COMMAND ----------

df=df.filter(col("Phone").cast(StringType()).like("%7"))
# df=df.filter(col("Phone").cast(IntegerType()).like("%7"))
df.count()
# df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Role of the Driver Node:
# MAGIC
# MAGIC The **Driver Node** is the main process that controls the execution of a Spark job. It does the following:
# MAGIC
# MAGIC 1. **Receives the user's query**:
# MAGIC    - The driver is responsible for accepting the Spark code (queries, transformations, and actions) written by the user, typically in the form of a Spark job.
# MAGIC
# MAGIC 2. **Creates the DAG (Directed Acyclic Graph)**:
# MAGIC    - The driver takes the code and converts it into a **DAG** that represents the sequence of operations (transformations) that need to be performed.
# MAGIC
# MAGIC 3. **Plans and schedules the execution**:
# MAGIC    - Once the DAG is created, the driver schedules tasks and breaks them down into smaller operations that can be executed across the Spark cluster.
# MAGIC
# MAGIC 4. **Distributes tasks**:
# MAGIC    - The driver sends the tasks (divided by partitions) to the **Worker Nodes (Executors)** for parallel execution.
# MAGIC
# MAGIC 5. **Collects results**:
# MAGIC    - After execution, the driver aggregates the results of the tasks and sends them back to the user.
# MAGIC
# MAGIC ---
# MAGIC  - we have only 4 task in the diagram becasue for each executer node in our databricks cluster we have 4 cores to execute out task 
# MAGIC  
# MAGIC ```plaintext
# MAGIC +----------------------------------+
# MAGIC |      User Query:                |
# MAGIC |      df.filter(col("Phone").cast(StringType()).like("%7")).count() |
# MAGIC +----------------------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC          +------------------+ 
# MAGIC          |   Driver Node    |        (Creates DAG, plans the execution)
# MAGIC          +------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC     +-----------------------------+
# MAGIC     |      Stage 1: Filter Rows    | (Filter rows where phone number ends with '7')
# MAGIC     +-----------------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC       +-------------------------+     
# MAGIC       | Task 1 (Partition 1)     |        (Filter on partition 1)
# MAGIC       +-------------------------+   
# MAGIC                   |
# MAGIC                   v
# MAGIC       +-------------------------+     
# MAGIC       | Task 2 (Partition 2)     |        (Filter on partition 2)
# MAGIC       +-------------------------+ 
# MAGIC                   |
# MAGIC                   v
# MAGIC       +-------------------------+     
# MAGIC       | Task 3 (Partition 3)     |        (Filter on partition 3)
# MAGIC       +-------------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC       +-------------------------+     
# MAGIC       | Task 4 (Partition 4)     |        (Filter on partition 4)
# MAGIC       +-------------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC     +-----------------------------+
# MAGIC     |   Action: count()           |        (Collect the results and count the filtered rows)
# MAGIC     +-----------------------------+
# MAGIC                   |
# MAGIC                   v
# MAGIC       +-------------------------+     
# MAGIC       | Final Result (Count)     |        (Send final count back to user)
# MAGIC       +-------------------------+ 
# MAGIC  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps in the DAG Creation:
# MAGIC
# MAGIC 1. **The Driver reads the user query**:
# MAGIC    - The driver receives the user's query (e.g., `df.filter(col("Phone").cast(StringType()).like("%7"))`).
# MAGIC
# MAGIC 2. **The DAG for this query consists of one stage**:
# MAGIC    - Since there’s no data shuffling involved in the `filter` operation, the DAG for this query is a **single stage**.
# MAGIC
# MAGIC 3. **Stage 1**:
# MAGIC    - This stage involves the `filter` transformation, which checks if the `Phone` column ends with '7'. 
# MAGIC    - This operation is a **narrow transformation**, meaning it does not require data shuffling between partitions. 
# MAGIC    - Hence, Spark executes this operation in a single stage, and it can be split across multiple tasks running on different partitions.
# MAGIC

# COMMAND ----------

# output of this command show the dag whihch will be generate in the back end based on our logical and physical plan 
df.filter(col("Phone").cast(StringType()).like("%7")).explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalyst Optimizer Breakdown
# MAGIC
# MAGIC #### 1. **Analysis**
# MAGIC - **Purpose**: The query is parsed and translated into a **Logical Plan**.
# MAGIC - **Steps**:
# MAGIC   - Spark begins by analyzing the user query.
# MAGIC   - It parses the query to determine the structure of the operations and the underlying data.
# MAGIC   - This results in a **Logical Plan**: a high-level, abstract representation of the transformations (such as filters, joins, etc.) that need to be applied to the data.
# MAGIC   
# MAGIC   **Example**: 
# MAGIC   If the user writes `df.filter(col("Phone").like("%7")).count()`, the logical plan would describe the filtering and counting operations that need to be performed on the DataFrame `df`.
# MAGIC
# MAGIC #### 2. **Logical Optimization**
# MAGIC - **Purpose**: Apply **rule-based optimizations** to the Logical Plan to improve query efficiency.
# MAGIC - **Steps**:
# MAGIC   - The Catalyst Optimizer applies various transformations to the logical plan to improve its efficiency.
# MAGIC   - These transformations include:
# MAGIC     - **Predicate Pushdown**: Moving filters closer to the data source to reduce the amount of data read. For example, if the filter is on a column (e.g., `Phone`), it tries to push this filter operation to the database or data source, which can handle it more efficiently.
# MAGIC     - **Projection Pruning**: Removing unnecessary columns from the plan, ensuring that only the required columns are retrieved, thus saving memory and computation.
# MAGIC     - **Constant Folding**: Simplifying expressions involving constants at compile time. If an expression like `1 + 2` is present, it will be optimized to `3` before execution.
# MAGIC   
# MAGIC   **Outcome**: This step reduces the cost of computation by simplifying the logical operations, improving the query plan’s overall efficiency.
# MAGIC
# MAGIC
# MAGIC #### 3. **Physical Planning**
# MAGIC - **Purpose**: Create a **Physical Plan** based on the optimized logical plan.
# MAGIC - **Steps**:
# MAGIC   - Once the logical plan has been optimized, the Catalyst Optimizer generates one or more **Physical Plans**.
# MAGIC   - A **Physical Plan** specifies how the operations in the logical plan should be executed. It provides the detailed execution strategy, including how data will be read, processed, and written.
# MAGIC   - Key components of physical planning include:
# MAGIC     - **Join Strategy**: The physical plan will decide the most efficient join method to use (e.g., **broadcast join**, **shuffle join**, etc.) based on factors such as data size and distribution.
# MAGIC     - **Data Partitioning**: Data will be partitioned and distributed across worker nodes. The physical plan specifies how data will be partitioned and processed in parallel to optimize performance.
# MAGIC
# MAGIC   **Outcome**: The physical plan provides a concrete, executable strategy for Spark to follow.
# MAGIC
# MAGIC
# MAGIC #### 4. **Code Generation**
# MAGIC - **Purpose**: Generate **executable code** based on the physical plan for execution on Spark.
# MAGIC - **Steps**:
# MAGIC   - The final step in the Catalyst Optimizer is to generate **executable code** for the physical plan.
# MAGIC   - Spark uses **Tungsten** for code generation. This involves creating optimized JVM bytecode for each operation (like filtering, aggregating, etc.), ensuring that the operations are executed efficiently on the cluster.
# MAGIC   - **Spark Jobs Code**: This code is sent to the cluster to be executed across the worker nodes.
# MAGIC   
# MAGIC   **Outcome**: Spark creates efficient, low-level code that is optimized for execution on the underlying hardware (such as CPUs, memory, and network).
# MAGIC
# MAGIC
# MAGIC ### Diagram Overview :
# MAGIC
# MAGIC ```plaintext
# MAGIC +-------------------+
# MAGIC | Catalyst Optimizer|  <-- Optimizes the query using various transformations
# MAGIC |                   |
# MAGIC |   +-------------+ |
# MAGIC |   |  Analysis   | |  <-- Parse query and create a logical plan
# MAGIC |   | - Logical Plan| |
# MAGIC |   +-------------+ |
# MAGIC |         |         |
# MAGIC |         v         |
# MAGIC |   +-------------+ |
# MAGIC |   | Logical     | |  <-- Apply rule-based optimizations
# MAGIC |   | Optimization| |
# MAGIC |   | - Predicate Pushdown| |
# MAGIC |   | - Projection Pruning| |
# MAGIC |   | - Constant Folding| |
# MAGIC |   +-------------+ |
# MAGIC |         |         |
# MAGIC |         v         |
# MAGIC |   +-------------+ |
# MAGIC |   | Physical    | |  <-- Generate physical plans based on optimizations
# MAGIC |   | Planning    | |
# MAGIC |   | - Join Strategy | |
# MAGIC |   | - Data Partitioning| |
# MAGIC |   +-------------+ |
# MAGIC |         |         |
# MAGIC |         v         |
# MAGIC |   +-------------+ |
# MAGIC |   | Code        | |  <-- Generate executable code for execution
# MAGIC |   | Generation  | |
# MAGIC |   | - Spark Jobs Code| |
# MAGIC |   +-------------+ |
# MAGIC +-------------------+
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Executor Memory Allocation Breakdown (8 GB Executor)
# MAGIC
# MAGIC #### 1. **Reserved Memory (300 MB)**:
# MAGIC    - This portion is set aside for **internal use** by Spark or JVM overhead.
# MAGIC    - **Not available** for user tasks or Spark execution/storage.
# MAGIC    - **Example**: This memory might be used for managing Spark's internal state, JVM overhead, or system tasks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. **Spark Memory (50% of total memory)**:
# MAGIC    - The total memory allocated to each executor for tasks is **8 GB**.
# MAGIC    - The Spark memory is **divided** into two parts:
# MAGIC    
# MAGIC    - ##### **Execution Memory (60% of 8 GB = 4.8 GB)**:
# MAGIC      - **Purpose**: Used for computation tasks like shuffling, sorting, filtering, and joining data.
# MAGIC      - **Example Task**: For the task `df.filter(col("Phone").like("%7")).count()`, Spark:
# MAGIC        - Filters the rows in the DataFrame where the "Phone" column contains the digit "7".
# MAGIC        - **Memory Use**: Execution memory is used to store temporary results while performing this filtering task.
# MAGIC        - **Memory Allocated**: **4.8 GB** (60% of 8 GB) is used for this task's execution.
# MAGIC    
# MAGIC    - ##### **Storage Memory (40% of 8 GB = 3.2 GB)**:
# MAGIC      - **Purpose**: Used for **caching** or **persisting** data (e.g., RDDs, DataFrames) for future stages or reuse.
# MAGIC      - **Example Task**: If you cache the DataFrame `df`, it will occupy the storage memory:
# MAGIC        - `df.cache()` stores the data in memory for faster access across stages.
# MAGIC        - **Memory Use**: Storage memory is allocated to hold the DataFrame or filtered data if cached.
# MAGIC        - **Memory Allocated**: **3.2 GB** (40% of 8 GB) is available for storing this cached data.
# MAGIC
# MAGIC - #### **Note**
# MAGIC     - Execution Memory or Storage Memory can be increased or decresed based on the requriments
# MAGIC ---
# MAGIC
# MAGIC #### 3. **Slots**:
# MAGIC    - The **Spark memory** is divided into multiple **slots** (`slot1`, `slot2`, `slot3`, etc.).
# MAGIC    - Each **slot** is used for parallel task execution.
# MAGIC    - **Example Task**: For the `df.filter(col("Phone").like("%7")).count()` operation, the task might be split into multiple partitions, and each partition is processed in parallel using a separate slot.
# MAGIC      - If there are 4 partitions in the DataFrame, Spark could assign each partition to a separate slot, allowing the task to run **concurrently**.
# MAGIC   
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4. **User Memory (40% of total memory = 3.2 GB)**:
# MAGIC    - Represents the **memory allocated** for the user’s data and tasks.
# MAGIC    - The user memory is used to store **intermediate data** and **results** for the task.
# MAGIC    - **Example Task**: For `df.filter(col("Phone").like("%7")).count()`, the filtered data (matching "Phone" values with "7") will be temporarily stored in user memory before counting the rows.
# MAGIC    - **Memory Allocated**: **3.2 GB** (40% of 8 GB) is available for the user’s data and task results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Concept:
# MAGIC - ### **Unified Memory Manager**: 
# MAGIC   - Allows Spark to **dynamically allocate unused memory** between **execution** and **storage memory**, depending on which one is underutilized.
# MAGIC   - **For example**: If execution memory is not fully used, Spark can allocate some of it to storage memory (for caching), improving resource utilization.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Example Scenario (8 GB Executor):
# MAGIC
# MAGIC Let’s say we have **8 GB of memory allocated** to each executor. Here’s how the memory is allocated based on the task `df.filter(col("Phone").like("%7")).count()`:
# MAGIC
# MAGIC 1. **Reserved Memory (300 MB)**:
# MAGIC    - **300 MB** is reserved for Spark’s internal overhead. This memory will not be used for user tasks like filtering.
# MAGIC
# MAGIC 2. **Executor Memory (50% = 4 GB)**:
# MAGIC    - **Execution Memory (60% = 4.8 GB)**: This memory will handle the computation for filtering the DataFrame based on the "Phone" column containing "7".
# MAGIC    - **Storage Memory (40% = 3.2 GB)**: If the filtered DataFrame is cached for future stages or reuse, it will be stored in the 3.2 GB of storage memory.
# MAGIC
# MAGIC 3. **Slots**:
# MAGIC    - The executor memory (8 GB) is divided into **4 slots** for parallel execution.
# MAGIC    - **Each slot** gets **2 GB** of memory, and each slot processes a partition of the DataFrame in parallel.
# MAGIC      - For example, if the DataFrame `df` has 4 partitions, Spark may process each partition in parallel across 4 slots.
# MAGIC
# MAGIC 4. **User Memory (40% = 3.2 GB)**:
# MAGIC    - The task `df.filter(col("Phone").like("%7")).count()` will utilize **3.2 GB** of user memory to store intermediate results and final results.
# MAGIC    - After the filtering, the count of matching rows is stored in the user memory.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Memory Management with Unified Memory Manager:
# MAGIC - The **Unified Memory Manager** in Spark ensures that unused memory from **Execution Memory** and **Storage Memory** can be dynamically shared.
# MAGIC - If the **Execution Memory** is underutilized while filtering, it can be reallocated for **Storage Memory**, allowing more data to be cached.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Final Summary:
# MAGIC - **Reserved Memory**: 300 MB for Spark's internal overhead.
# MAGIC - **Executor Memory** (8 GB): Divided into **Execution Memory** (4.8 GB) for processing tasks and **Storage Memory** (3.2 GB) for caching.
# MAGIC - **Slots**: The executor can process tasks in parallel using 4 slots, each with 2 GB of memory.
# MAGIC - **User Memory**: 3.2 GB is used for user data during task execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Diagram Overview :  
# MAGIC
# MAGIC
# MAGIC ```plaintext
# MAGIC
# MAGIC +-------------------------------+
# MAGIC |   reserved memeory 300mb      |
# MAGIC |-------------------------------|<--+      +--------------------------------+
# MAGIC |+-----------------------------+|   |      |        |       |       |       |
# MAGIC ||                             ||   |      | slot1  | slot2 | slot3 | slot4 | 
# MAGIC ||   storage memeory (50%)     ||   |      |        |       |       |       | 
# MAGIC ||                             ||   |      +--------------------------------+
# MAGIC |+-----------------------------+|   |------->(spark memory 60%)it has slots to execute our code      
# MAGIC ||                             ||   |
# MAGIC ||   Executer memeory (50%)    ||   |
# MAGIC ||                             ||   |
# MAGIC |+-----------------------------+|   |
# MAGIC |-------------------------------|<--+
# MAGIC |                               |
# MAGIC |   user memeory 40%            |
# MAGIC |                               |
# MAGIC |                               |
# MAGIC +-------------------------------+
# MAGIC
