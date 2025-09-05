function tileToLngLat(tileX, tileY, tilesPerSide = 2048) {
    const xNorm = tileX / tilesPerSide;
    const yNorm = tileY / tilesPerSide;

    const lng = xNorm * 360 - 180;
    const lat = Math.atan(Math.sinh(Math.PI * (1 - 2 * yNorm))) * 180 / Math.PI;

    return [lng, lat];
}


let coord = tileToLngLat(685, 668);
console.log(coord);
console.log(`https://wplace.live/?lat=${coord[1]}&lng=${coord[0]}&zoom=15`);