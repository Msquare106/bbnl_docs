console.log("Script Connected");

function update_time(){
    let d = new Date();
    let datetime = d.toDateString()+", "+d.toLocaleTimeString('en-US', { hour12: true, hour24: false }).padStart(11, '0');
    document.querySelector(".time-date").innerHTML = datetime;
}

// setInterval(update_time, 1000);

let side_bar = document.getElementById("side_bar");
function sidebar_toggle_func(){
    // let side_bar = document.getElementById("side_bar");
    side_bar.classList.toggle("-translate-x-full");
    side_bar.classList.toggle("fixed");

    let sidebar_toggle = document.getElementById("sidebar_toggle");
    sidebar_toggle.classList.toggle("flex-row-reverse");
    sidebar_toggle.classList.toggle("translate-x-full");

    let cn = sidebar_toggle.querySelector("p");
    cn.classList.toggle("group-hover:-translate-x-18");
    cn.classList.toggle("group-hover:translate-x-18");

    let l = ["Hide Options", "View Options"];
    cn.innerHTML = l[1 - l.indexOf(cn.innerHTML)];

    let ico = sidebar_toggle.querySelector("i");
    ico.classList.toggle("fa-angle-left");
    ico.classList.toggle("fa-angle-right");
    ico.classList.toggle("translate-x-24");
    ico.classList.toggle("-translate-x-24");
    ico.classList.toggle("group-hover:translate-x-18");
    ico.classList.toggle("group-hover:-translate-x-18");
}

function view_content(event){
    let clicked_opt_index = Number(event.target.dataset.index);
    let contents = document.querySelectorAll(".main-container .content-container .content");
    contents.forEach((c)=>{
        c.classList.remove("show-content");
        c.classList.add("hide-content");
        if(c === contents[clicked_opt_index]){
            c.classList.remove("hide-content");
            c.classList.add("show-content");
        }
    })
}

window.addEventListener('load', ()=>{
    document.getElementById("tailwindcss_style").innerHTML = style_html;
    document.querySelector(".side-bar .nav-options .default").click();
    setTimeout(()=>{
        document.getElementById("tailwindcss_style").innerHTML += `.content-container .content{
            @apply transition-all duration-400 ease-in-out;
        }`;
    })
})

// Map Pan/Zoom

const svg_wrapper = document.querySelector('.svg-wrapper');
const panzoom = Panzoom(svg_wrapper, {
    transformOrigin: '0 0',
    maxScale: 6,
    minScale: 1,
    step: 0.25
});
svg_wrapper.parentElement.addEventListener('wheel', panzoom.zoomWithWheel);

// End of Map Pan/Zoom

// Map Info Hover Code

let state_info_box = document.querySelector(".content-container .content .map-svg .state-info");
let map_container = document.querySelector(".content-container .content .map-svg");
let map_svg = document.querySelector(".content-container .content .map-svg #svg2");

function show_hide_state_info(remove, add){
    state_info_box.classList.replace(`scale-y-[${remove}]`, `scale-y-[${add}]`);
    state_info_box.classList.replace(`opacity-[${remove}]`, `opacity-[${add}]`);
}

function hover_state_info(cursor){
    if(cursor.target.tagName == "path"){
        show_hide_state_info(0, 1);
        let cursorx_gap = 12;
        let cursory_gap = -(state_info_box.clientHeight + 42);
        if(!side_bar.classList.contains("-translate-x-full")){
            cursorx_gap = -(side_bar.clientWidth - 12);
        }
        state_info_box.style.left = `${cursor.pageX + (cursorx_gap)}px`;
        state_info_box.style.top = `${cursor.pageY + (cursory_gap)}px`;
        let state_info_bound = state_info_box.getBoundingClientRect();
        let map_container_bound = map_container.getBoundingClientRect();
        if(state_info_bound.right > (map_container_bound.right - (state_info_box.clientWidth / 2))){
            state_info_box.style.left = `${cursor.pageX + (cursorx_gap - (state_info_box.clientWidth + 24))}px`;
        }
        if(state_info_bound.top < (map_container_bound.top + (state_info_box.clientHeight / 2))){
            state_info_box.style.top = `${cursor.pageY + (cursory_gap + (state_info_box.clientHeight + 21))}px`;
        }
    }else{
        show_hide_state_info(1, 0);
    }
}

function get_state_info_content(cursor){
    if(cursor.target.tagName == "path"){
        let state_name = String(cursor.target.dataset.state_name).toUpperCase();
        state_info_box.querySelector("h1").innerHTML = state_name;
    }
}

document.addEventListener('mouseleave', ()=>{
    show_hide_state_info(1, 0);
})

// Map Info Hover Code End