<template>
    <div class="container">
    <table id="tableComponent" class="table table-bordered table-striped data-table">
        <thead class="data-table">
            <tr>
                <!-- loop through each value of the fields to get the table header -->
                <th v-for="field in fields" :key='field' >
                    {{ field }} <i class="bi bi-sort-alpha-down" aria-label='Sort Icon'></i>
                </th>
            </tr>
        </thead>
        <tbody>
            <tr v-for="item in data" :key='item._index'>
                <td v-for="field in fields" :key='field'>{{ item[field] }}</td>
            </tr>
        </tbody>
    </table>
    </div>
</template>
<style>

.data-table {
    width: 100%;
    min-width: 100%;
    margin-bottom: 1rem;
    /* color: #212529; */
    border-collapse: collapse;
}

.container {
  display: flex;
  justify-content: center; /* Horizontally center */
  align-items: center; /* Vertically center */
}

</style>
<script>
export default {
    name: 'TableComponent',
    props: {
        // 
        data: {
            type: Array,
        },
        fields: {
            type: Array,
        }
    },
}
</script>