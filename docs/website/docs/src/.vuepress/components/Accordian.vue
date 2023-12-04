<template>
    <li class="panel container mb-4 border-2 rounded-lg shadow-sm">
        <button :arial-controls="'accordion-content-' + title" :id="'accordion-control-' + title"
            @click.prevent="togglePanel"
            class="p-4 w-full border-b-2 font-semibold flex flex-row items-center justify-between">
            {{ title }}
        </button>
        <div :aria-hidden="!showPanel" :id="'content-' + title" class="p-4" v-if="showPanel">
            <slot></slot>
        </div>
    </li>
</template>

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