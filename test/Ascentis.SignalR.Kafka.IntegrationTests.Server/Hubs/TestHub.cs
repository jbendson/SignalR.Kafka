using System.Threading.Tasks;
using Microsoft.AspNetCore.SignalR;

namespace Ascentis.SignalR.Kafka.IntegrationTests.Server.Hubs;

public class TestHub : Hub
{
    public async Task SendAll(string message)
    {
        await Clients.All.SendAsync("ReceiveMessage", message);
    }

    public async Task SendAllExcept(string[] connectionIds, string message)
    {
        await Clients.AllExcept(connectionIds).SendAsync("ReceiveMessage", message);
    }

    public async Task SendConnection(string connectionId, string message)
    {
        await Clients.Client(connectionId).SendAsync("ReceiveMessage", message);
    }

    public async Task AddGroup(string group)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, group);
    }

    public async Task AddGroupConnection(string group, string connectionId)
    {
        await Groups.AddToGroupAsync(connectionId, group);
    }

    public async Task RemoveGroup(string group)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
    }

    public async Task RemoveGroupConnection(string group, string connectionId)
    {
        await Groups.RemoveFromGroupAsync(connectionId, group);
    } 

    public async Task SendGroup(string group, string message)
    {
        await Clients.Groups(group).SendAsync("ReceiveMessage", message);
    }

    public async Task SendGroups(string[] groups, string message)
    {
        await Clients.Groups(groups).SendAsync("ReceiveMessage", message);
    }

    public async Task SendGroupExcept(string[] connectionIds, string group, string message)
    {
        await Clients.GroupExcept(group, connectionIds).SendAsync("ReceiveMessage", message);
    }

    public async Task SendOthersInGroup(string group, string message)
    {
        await Clients.OthersInGroup(group).SendAsync("ReceiveMessage", message);
    }

    public async Task SendUser(string user, string message)
    {
        await Clients.User(user).SendAsync("ReceiveMessage", message);
    }

    public async Task SendUsers(string[] users, string message)
    {
        await Clients.Users(users).SendAsync("ReceiveMessage", message);
    }
}