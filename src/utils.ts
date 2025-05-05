export function sleep (howLong: number) {
    return new Promise((resolve) => {
      setTimeout(resolve, howLong)
    })
  };
  
  export function randomInRange (min: number, max: number, wholeNumber: boolean = false) {
    const num = Math.random() * (max - min) + min;
    return wholeNumber ? Math.floor(num) : Math.round(num * 10) / 10;
  };