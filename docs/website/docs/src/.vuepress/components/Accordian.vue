<template>
    <li class="panel flex container mb-4 border-2 rounded-lg shadow-sm">
        <button :arial-controls="'accordion-content-' + title" :id="'accordion-control-' + title"
            @click.prevent="togglePanel"
            class="button flex">
            {{ title }}
        </button>
        <div :aria-hidden="!showPanel" :id="'content-' + title" class="flex" v-if="showPanel">
            <slot></slot>
        </div>
    </li>
</template>
<style>
.button {
    width: 100%;
}
.flex {
    display: flex;
    flex-direction: column;
}
.panel {
    border: .5px solid #ddd;
    border-radius: 4px;
    margin-bottom: 16px;
    overflow: hidden;
}
</style>

<script setup>
import { ref } from 'vue'

const props = defineProps({
    title: { type: String, required: true },
});

const showPanel = ref(false);

const togglePanel = (event) => {
    showPanel.value = !showPanel.value;
}
</script>