export const handler = async () => {
  await sleep(); 

  const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from a task Lambda!'),
  };
  return response;
};

// sleep for x milliseconds, simulating a task that takes some time complete
async function sleep() {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve('sleep complete');
    }, 2000);
  });
}
